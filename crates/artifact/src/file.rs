//! File artifact：固定目录句柄、no-follow、内容/身份 CAS 与不可覆盖的原子发布。
use agent_core::{ArtifactError, ArtifactIdentity, MAX_ARTIFACT_BYTES};
use rustix::fs::{self, AtFlags, Mode, OFlags, RenameFlags};
use rustix::io::Errno;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{File, Metadata};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct FileArtifactDirectory {
    path: PathBuf,
    parent: File,
    name: OsString,
    bound: Mutex<Option<File>>,
}
/// 私有临时对象能力；丢弃时沿原目录句柄清理，不按字符串重新打开。
pub struct StagedFile {
    directory: File,
    temporary: String,
    name: String,
    pub identity: ArtifactIdentity,
}
impl Drop for StagedFile {
    fn drop(&mut self) {
        let _ = fs::unlinkat(&self.directory, self.temporary.as_str(), AtFlags::empty());
    }
}
fn unavailable<T>(_: T) -> ArtifactError {
    ArtifactError::StorageUnavailable
}
fn filesystem(error: Errno) -> ArtifactError {
    if error == Errno::LOOP || error == Errno::NOTDIR {
        ArtifactError::Forbidden
    } else {
        ArtifactError::StorageUnavailable
    }
}
fn version(meta: &Metadata) -> (u64, u64, u64, i64, i64, i64, i64, u64) {
    (
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
        meta.nlink(),
    )
}
fn same_node(a: &File, b: &File) -> Result<bool, ArtifactError> {
    let a = a.metadata().map_err(unavailable)?;
    let b = b.metadata().map_err(unavailable)?;
    Ok(a.dev() == b.dev() && a.ino() == b.ino())
}
fn digest_name(name: &str) -> Result<&str, ArtifactError> {
    let digest = name
        .strip_prefix("sha256-")
        .and_then(|s| s.strip_suffix(".txt"))
        .ok_or(ArtifactError::InvalidReference)?;
    if !(ArtifactIdentity {
        sha256: digest.into(),
        bytes: 0,
    })
    .valid()
    {
        return Err(ArtifactError::InvalidReference);
    }
    Ok(digest)
}
fn inspect(
    file: &mut File,
    offset: u64,
    count: usize,
) -> Result<(ArtifactIdentity, Vec<u8>), ArtifactError> {
    let before = file.metadata().map_err(unavailable)?;
    if !before.is_file() || before.nlink() != 1 {
        return Err(ArtifactError::Forbidden);
    }
    if before.len() > MAX_ARTIFACT_BYTES {
        return Err(ArtifactError::InvalidReference);
    }
    let mut hasher = Sha256::new();
    let mut position = 0u64;
    let mut bytes = Vec::new();
    let end = offset.saturating_add(count.clamp(1, 16384) as u64);
    let mut chunk = [0u8; 65536];
    loop {
        let read = file.read(&mut chunk).map_err(unavailable)?;
        if read == 0 {
            break;
        }
        let next = position
            .checked_add(read as u64)
            .filter(|n| *n <= MAX_ARTIFACT_BYTES)
            .ok_or(ArtifactError::InvalidReference)?;
        hasher.update(&chunk[..read]);
        let start = offset.max(position);
        let stop = end.min(next);
        if start < stop {
            bytes
                .extend_from_slice(&chunk[(start - position) as usize..(stop - position) as usize]);
        }
        position = next;
    }
    if version(&before) != version(&file.metadata().map_err(unavailable)?) {
        return Err(ArtifactError::Conflict);
    }
    Ok((
        ArtifactIdentity {
            sha256: format!("{:x}", hasher.finalize()),
            bytes: position,
        },
        bytes,
    ))
}
impl FileArtifactDirectory {
    pub fn store_id(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(format!("local-file-v1:{}", self.path.display()).as_bytes())
        )
    }
    pub fn reference(&self, identity: &ArtifactIdentity) -> Result<String, ArtifactError> {
        if !identity.valid() {
            return Err(ArtifactError::InvalidReference);
        }
        Ok(self
            .path
            .join(format!("sha256-{}.txt", identity.sha256))
            .to_string_lossy()
            .into_owned())
    }

    pub fn new(path: PathBuf) -> Result<Self, ArtifactError> {
        let parent_path = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = File::from(
            fs::open(
                parent_path,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(filesystem)?,
        );
        let name = path
            .file_name()
            .ok_or(ArtifactError::InvalidReference)?
            .to_owned();
        Ok(Self {
            path,
            parent,
            name,
            bound: Mutex::new(None),
        })
    }
    fn directory(&self, create: bool) -> Result<Option<File>, ArtifactError> {
        if create {
            match fs::mkdirat(&self.parent, &self.name, Mode::from_raw_mode(0o700)) {
                Ok(()) => self.parent.sync_all().map_err(unavailable)?,
                Err(Errno::EXIST) => {}
                Err(error) => return Err(filesystem(error)),
            }
        }
        let current = match fs::openat(
            &self.parent,
            &self.name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => File::from(fd),
            Err(Errno::NOENT) if !create => return Ok(None),
            Err(error) => return Err(filesystem(error)),
        };
        let mut bound = self.bound.lock().map_err(unavailable)?;
        if let Some(original) = bound.as_ref() {
            if !same_node(original, &current)? {
                return Err(ArtifactError::Conflict);
            }
        } else {
            *bound = Some(current.try_clone().map_err(unavailable)?);
        }
        Ok(Some(current))
    }
    fn validate_directory(&self, expected: &File) -> Result<(), ArtifactError> {
        let current = self.directory(false)?.ok_or(ArtifactError::Conflict)?;
        if !same_node(expected, &current)? {
            return Err(ArtifactError::Conflict);
        }
        Ok(())
    }
    fn open_file(directory: &File, name: &str) -> Result<Option<File>, ArtifactError> {
        digest_name(name)?;
        match fs::openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => Ok(Some(File::from(fd))),
            Err(Errno::NOENT) => Ok(None),
            Err(error) => Err(filesystem(error)),
        }
    }
    fn verify_existing(
        &self,
        directory: &File,
        name: &str,
        expected: &ArtifactIdentity,
    ) -> Result<(), ArtifactError> {
        let mut file = Self::open_file(directory, name)?.ok_or(ArtifactError::Corrupt)?;
        let initial = file.metadata().map_err(unavailable)?;
        let (actual, _) = inspect(&mut file, 0, 1)?;
        if actual != *expected {
            return Err(ArtifactError::Corrupt);
        }
        let current = Self::open_file(directory, name)?.ok_or(ArtifactError::Conflict)?;
        if version(&initial) != version(&current.metadata().map_err(unavailable)?) {
            return Err(ArtifactError::Conflict);
        }
        self.validate_directory(directory)
    }
    pub fn stage(&self, content: &str) -> Result<StagedFile, ArtifactError> {
        if content.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::InvalidReference);
        }
        let identity = ArtifactIdentity {
            sha256: format!("{:x}", Sha256::digest(content.as_bytes())),
            bytes: content.len() as u64,
        };
        let directory = self
            .directory(true)?
            .ok_or(ArtifactError::StorageUnavailable)?;
        let name = format!("sha256-{}.txt", identity.sha256);
        let temporary = format!(
            ".{}-{}-{}.tmp",
            identity.sha256,
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut file = File::from(
            fs::openat(
                &directory,
                temporary.as_str(),
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .map_err(filesystem)?,
        );
        let staged = StagedFile {
            directory,
            temporary,
            name,
            identity,
        };
        file.write_all(content.as_bytes()).map_err(unavailable)?;
        file.sync_all().map_err(unavailable)?;
        // 摘要来自实际临时对象，不能只信调用者提供的元数据。
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).map_err(unavailable)?;
        let (actual, _) = inspect(&mut file, 0, 1)?;
        if actual != staged.identity {
            return Err(ArtifactError::Corrupt);
        }
        self.validate_directory(&staged.directory)?;
        Ok(staged)
    }
    pub fn finalize(&self, staged: StagedFile) -> Result<String, ArtifactError> {
        self.validate_directory(&staged.directory)?;
        match fs::renameat_with(
            &staged.directory,
            staged.temporary.as_str(),
            &staged.directory,
            staged.name.as_str(),
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(error) => return Err(filesystem(error)),
        }
        fs::unlinkat(
            &staged.directory,
            staged.temporary.as_str(),
            AtFlags::empty(),
        )
        .or_else(|e| if e == Errno::NOENT { Ok(()) } else { Err(e) })
        .map_err(filesystem)?;
        staged.directory.sync_all().map_err(unavailable)?;
        self.verify_existing(&staged.directory, &staged.name, &staged.identity)?;
        Ok(self.path.join(&staged.name).to_string_lossy().into_owned())
    }
    pub fn publish(&self, content: &str) -> Result<String, ArtifactError> {
        self.finalize(self.stage(content)?)
    }
    pub fn read(
        &self,
        reference: &str,
        offset: u64,
        count: usize,
    ) -> Result<(ArtifactIdentity, Vec<u8>), ArtifactError> {
        let path = Path::new(reference);
        if path.parent() != Some(self.path.as_path()) {
            return Err(ArtifactError::InvalidReference);
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(ArtifactError::InvalidReference)?;
        let expected = digest_name(name)?;
        let directory = self
            .directory(false)?
            .ok_or(ArtifactError::StorageUnavailable)?;
        let mut file =
            Self::open_file(&directory, name)?.ok_or(ArtifactError::StorageUnavailable)?;
        let initial = file.metadata().map_err(unavailable)?;
        let (actual, bytes) = inspect(&mut file, offset, count)?;
        if actual.sha256 != expected {
            return Err(ArtifactError::Corrupt);
        }
        let current = Self::open_file(&directory, name)?.ok_or(ArtifactError::Conflict)?;
        if version(&initial) != version(&current.metadata().map_err(unavailable)?) {
            return Err(ArtifactError::Conflict);
        }
        self.validate_directory(&directory)?;
        Ok((actual, bytes))
    }
    pub fn candidates(&self) -> Result<Vec<PathBuf>, ArtifactError> {
        let Some(directory) = self.directory(false)? else {
            return Ok(vec![]);
        };
        let entries = fs::Dir::read_from(&directory).map_err(filesystem)?;
        let mut names = Vec::new();
        for entry in entries.take(1000) {
            let entry = entry.map_err(filesystem)?;
            let Ok(name) = entry.file_name().to_str() else {
                continue;
            };
            if digest_name(name).is_ok() {
                names.push(self.path.join(name));
            }
        }
        self.validate_directory(&directory)?;
        Ok(names)
    }
    pub fn clean_abandoned_temporary(
        &self,
        retention: std::time::Duration,
    ) -> Result<usize, ArtifactError> {
        let Some(directory) = self.directory(false)? else {
            return Ok(0);
        };
        // 活动 stage 不按零 retention 清理；崩溃残留只在一天后进入清理候选。
        let retention = retention.max(std::time::Duration::from_secs(86400));
        let mut removed = 0;
        for entry in fs::Dir::read_from(&directory)
            .map_err(filesystem)?
            .take(1000)
        {
            let entry = entry.map_err(filesystem)?;
            let Ok(name) = entry.file_name().to_str() else {
                continue;
            };
            let Some(middle) = name.strip_prefix('.').and_then(|n| n.strip_suffix(".tmp")) else {
                continue;
            };
            let mut parts = middle.split('-');
            let (Some(hash), Some(pid), Some(serial)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            if parts.next().is_some()
                || !(ArtifactIdentity {
                    sha256: hash.into(),
                    bytes: 0,
                })
                .valid()
                || pid.is_empty()
                || serial.is_empty()
                || !pid.bytes().all(|b| b.is_ascii_digit())
                || !serial.bytes().all(|b| b.is_ascii_digit())
            {
                continue;
            }
            let file = File::from(
                fs::openat(
                    &directory,
                    name,
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(filesystem)?,
            );
            let initial = file.metadata().map_err(unavailable)?;
            if !initial.is_file() || initial.nlink() != 1 {
                return Err(ArtifactError::Forbidden);
            }
            if initial
                .modified()
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_none_or(|age| age < retention)
            {
                continue;
            }
            let current = File::from(
                fs::openat(
                    &directory,
                    name,
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(filesystem)?,
            );
            if version(&initial) != version(&current.metadata().map_err(unavailable)?) {
                return Err(ArtifactError::Conflict);
            }
            self.validate_directory(&directory)?;
            fs::unlinkat(&directory, name, AtFlags::empty()).map_err(filesystem)?;
            removed += 1;
        }
        if removed > 0 {
            directory.sync_all().map_err(unavailable)?;
        }
        self.validate_directory(&directory)?;
        Ok(removed)
    }
    pub fn remove_if_old(
        &self,
        path: &Path,
        retention: std::time::Duration,
    ) -> Result<bool, ArtifactError> {
        if path.parent() != Some(self.path.as_path()) {
            return Err(ArtifactError::InvalidReference);
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(ArtifactError::InvalidReference)?;
        let expected = digest_name(name)?;
        let directory = self.directory(false)?.ok_or(ArtifactError::Conflict)?;
        let Some(mut file) = Self::open_file(&directory, name)? else {
            return Ok(false);
        };
        let initial = file.metadata().map_err(unavailable)?;
        if !initial.is_file()
            || initial.nlink() != 1
            || initial
                .modified()
                .ok()
                .and_then(|m| m.elapsed().ok())
                .is_none_or(|age| age < retention)
        {
            return Ok(false);
        }
        let (actual, _) = inspect(&mut file, 0, 1)?;
        if actual.sha256 != expected {
            return Err(ArtifactError::Corrupt);
        }
        let current = Self::open_file(&directory, name)?.ok_or(ArtifactError::Conflict)?;
        if version(&initial) != version(&current.metadata().map_err(unavailable)?) {
            return Err(ArtifactError::Conflict);
        }
        self.validate_directory(&directory)?;
        fs::unlinkat(&directory, name, AtFlags::empty()).map_err(filesystem)?;
        directory.sync_all().map_err(unavailable)?;
        Ok(true)
    }
}
