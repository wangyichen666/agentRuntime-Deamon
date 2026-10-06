//! 入口共享的单调展示游标；它不拥有业务状态或执行许可。
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const VISIBLE_VIEW_EVENTS: &[&str] = &[
    "run_started",
    "compact_terminal",
    "turn_started",
    "thinking_delta",
    "thinking_finished",
    "text_delta",
    "tool_started",
    "tool_finished",
    "approval_required",
    "assistant_content",
    "delegation_spawned",
    "delegation_terminal",
    "terminal",
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewStamp {
    pub schema_version: u16,
    pub session_key: SessionKey,
    pub session_lifetime_id: SessionLifetimeId,
    pub snapshot_revision: SnapshotRevision,
    pub metadata_revision: SnapshotRevision,
    pub transcript_revision: TranscriptSeq,
    pub projection_generation: ProjectionGeneration,
    pub owner: Option<ExactOwner>,
    pub event_seq: Option<EventSeq>,
    #[serde(default)]
    pub previous_visible_seq: Option<EventSeq>,
    pub terminal: bool,
    pub interaction: Option<ViewInteraction>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewInteraction {
    pub interaction_id: InteractionId,
    pub revision: i64,
    pub pending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewRun {
    pub owner: ExactOwner,
    pub event_seq: EventSeq,
    pub terminal: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewState {
    pub stamp: ViewStamp,
    pub retired_lifetimes: BTreeSet<String>,
    pub runs: BTreeMap<String, ViewRun>,
    pub interactions: BTreeMap<String, ViewInteraction>,
    pub history_modes: BTreeSet<String>,
    #[serde(default)]
    pub replay_cursors: BTreeMap<String, EventSeq>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ViewInput {
    Readback {
        snapshot: Box<SessionReadback>,
    },
    Run {
        stamp: Option<Box<ViewStamp>>,
        run: Box<RunRecord>,
    },
    Page {
        snapshot: Box<SessionReadback>,
        page_key: String,
    },
    Replay {
        stamp: Option<Box<ViewStamp>>,
        after_seq: EventSeq,
    },
    Event {
        stamp: Option<Box<ViewStamp>>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewDecision {
    Accepted,
    Ignored,
    Resync,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewReduction {
    pub state: Option<ViewState>,
    pub decision: ViewDecision,
    pub duplicate: bool,
}
impl ViewStamp {
    pub fn readback(snapshot: &SessionReadback) -> Self {
        Self {
            schema_version: 1,
            session_key: snapshot.session_id.clone(),
            session_lifetime_id: snapshot.session_lifetime_id.clone(),
            snapshot_revision: snapshot.snapshot_revision,
            metadata_revision: snapshot.metadata_revision,
            transcript_revision: snapshot.transcript_revision,
            projection_generation: snapshot.projection_generation,
            owner: None,
            event_seq: None,
            previous_visible_seq: None,
            terminal: false,
            interaction: None,
        }
    }
    pub fn valid(&self) -> bool {
        self.schema_version == 1
            && !self.session_key.0.is_empty()
            && self.session_key.0.len() <= 512
            && !self.session_lifetime_id.0.is_empty()
            && self.session_lifetime_id.0.len() <= 128
            && self.metadata_revision <= self.snapshot_revision
            && self.owner.is_some() == self.event_seq.is_some()
            && self
                .previous_visible_seq
                .is_none_or(|previous| self.event_seq.is_some_and(|seq| previous < seq))
            && (!self.terminal || self.owner.is_some())
            && self.owner.as_ref().is_none_or(|owner| {
                owner.session_key == self.session_key
                    && owner.session_lifetime_id == self.session_lifetime_id
                    && owner.run_generation.0 > 0
                    && !owner.run_id.0.is_empty()
                    && !owner.turn_id.0.is_empty()
            })
            && self
                .interaction
                .as_ref()
                .is_none_or(|item| !item.interaction_id.0.is_empty() && item.revision >= 0)
    }
    fn dominates(&self, old: &Self) -> bool {
        self.snapshot_revision >= old.snapshot_revision
            && self.metadata_revision >= old.metadata_revision
            && self.transcript_revision >= old.transcript_revision
            && self.projection_generation >= old.projection_generation
    }
}
/// 唯一时序规则。RPC与Rust客户端都调用此函数；失败仅要求重新读取。
pub fn reduce_view(state: Option<ViewState>, input: ViewInput) -> ViewReduction {
    if state.as_ref().is_some_and(|old| {
        !old.stamp.valid()
            || old.runs.len() > 128
            || old.interactions.len() > 256
            || old.history_modes.len() > 256
            || old.retired_lifetimes.len() > 128
            || old.replay_cursors.len() > 128
            || old.runs.iter().any(|(key, run)| {
                key != &run.owner.run_id.0
                    || run.owner.session_key != old.stamp.session_key
                    || run.owner.session_lifetime_id != old.stamp.session_lifetime_id
            })
    }) {
        return ViewReduction {
            state: None,
            decision: ViewDecision::Resync,
            duplicate: false,
        };
    }
    if let ViewInput::Run { stamp, run } = input {
        if stamp.as_deref().is_some_and(|stamp| {
            !stamp.valid()
                || stamp.owner.as_ref().is_none_or(|owner| {
                    owner.run_id != run.run_id
                        || owner.turn_id != run.turn_id
                        || owner.session_key != run.session_id
                })
                || stamp.event_seq != Some(run.last_seq)
        }) {
            return ViewReduction {
                state,
                decision: ViewDecision::Resync,
                duplicate: false,
            };
        }
        let known = state.as_ref().and_then(|view| view.runs.get(&run.run_id.0));
        let bound = known.is_some_and(|view| {
            view.owner.session_key == run.session_id
                && view.owner.turn_id == run.turn_id
                && view.event_seq == run.last_seq
                && view.terminal == run.status.terminal()
        });
        if bound
            && stamp
                .as_deref()
                .is_none_or(|stamp| stamp.owner.as_ref() == known.map(|known| &known.owner))
        {
            return ViewReduction {
                state,
                decision: ViewDecision::Ignored,
                duplicate: true,
            };
        }
        let Some(mut stamp) = stamp else {
            return ViewReduction {
                state,
                decision: ViewDecision::Resync,
                duplicate: false,
            };
        };
        if stamp.owner.as_ref().is_none_or(|owner| {
            owner.run_id != run.run_id
                || owner.turn_id != run.turn_id
                || owner.session_key != run.session_id
        }) || stamp.event_seq != Some(run.last_seq)
        {
            return ViewReduction {
                state,
                decision: ViewDecision::Resync,
                duplicate: false,
            };
        }
        stamp.terminal = run.status.terminal();
        if state.is_none() {
            let owner = stamp.owner.as_ref().cloned();
            if let Some(owner) = owner {
                let mut runs = BTreeMap::new();
                runs.insert(
                    owner.run_id.0.clone(),
                    ViewRun {
                        owner,
                        event_seq: run.last_seq,
                        terminal: run.status.terminal(),
                    },
                );
                let mut interactions = BTreeMap::new();
                if let Some(item) = &stamp.interaction {
                    interactions.insert(item.interaction_id.0.clone(), item.clone());
                }
                let mut replay_cursors = BTreeMap::new();
                replay_cursors.insert(run.run_id.0.clone(), run.last_seq);
                return ViewReduction {
                    state: Some(ViewState {
                        stamp: *stamp,
                        retired_lifetimes: BTreeSet::new(),
                        runs,
                        interactions,
                        history_modes: BTreeSet::new(),
                        replay_cursors,
                    }),
                    decision: ViewDecision::Accepted,
                    duplicate: false,
                };
            }
        }
        return reduce_view(state, ViewInput::Event { stamp: Some(stamp) });
    }

    let baseline = matches!(&input, ViewInput::Readback { .. } | ViewInput::Page { .. });
    let stamp = match &input {
        ViewInput::Readback { snapshot } | ViewInput::Page { snapshot, .. } => {
            Some(ViewStamp::readback(snapshot))
        }
        ViewInput::Event { stamp } | ViewInput::Replay { stamp, .. } => stamp.as_deref().cloned(),
        ViewInput::Run { .. } => None,
    };
    let answer = |decision, state| ViewReduction {
        state,
        decision,
        duplicate: false,
    };
    let Some(stamp) = stamp.filter(ViewStamp::valid) else {
        return answer(ViewDecision::Resync, state);
    };
    // durable replay 只补充基线已经证明仍活动的 run，不回退任何事实版本。
    if let (Some(old), ViewInput::Replay { after_seq, .. }, Some(owner), Some(seq)) =
        (&state, &input, &stamp.owner, stamp.event_seq)
    {
        if old.stamp.session_key == stamp.session_key
            && old.stamp.session_lifetime_id == stamp.session_lifetime_id
        {
            if let Some(run) = old.runs.get(&owner.run_id.0).filter(|run| {
                run.owner == *owner
                    && !run.terminal
                    && seq <= run.event_seq
                    && *after_seq <= run.event_seq
            }) {
                let cursor = old
                    .replay_cursors
                    .get(&owner.run_id.0)
                    .copied()
                    .unwrap_or(*after_seq);
                if seq <= cursor {
                    return answer(ViewDecision::Ignored, state);
                }
                if stamp.previous_visible_seq.map_or_else(
                    || cursor.0.checked_add(1) != Some(seq.0),
                    |previous| previous > cursor,
                ) {
                    return answer(ViewDecision::Resync, state);
                }
                let mut updated = old.clone();
                updated.replay_cursors.insert(owner.run_id.0.clone(), seq);
                let current = stamp.interaction.as_ref().is_none_or(|item| {
                    old.interactions.get(&item.interaction_id.0) == Some(item) && item.pending
                });
                let _ = run;
                return answer(
                    if current && !stamp.terminal {
                        ViewDecision::Accepted
                    } else {
                        ViewDecision::Ignored
                    },
                    Some(updated),
                );
            }
        }
    }
    let mut next = match &state {
        None if !baseline && stamp.previous_visible_seq != Some(EventSeq(0)) => {
            return answer(ViewDecision::Resync, state);
        }
        None => ViewState {
            stamp: stamp.clone(),
            retired_lifetimes: BTreeSet::new(),
            runs: BTreeMap::new(),
            interactions: BTreeMap::new(),
            history_modes: BTreeSet::new(),
            replay_cursors: BTreeMap::new(),
        },
        Some(old) => {
            if old.stamp.session_key != stamp.session_key
                || old.retired_lifetimes.contains(&stamp.session_lifetime_id.0)
            {
                return answer(ViewDecision::Ignored, state);
            }
            if old.stamp.session_lifetime_id != stamp.session_lifetime_id {
                if !baseline || stamp.snapshot_revision <= old.stamp.snapshot_revision {
                    return answer(ViewDecision::Ignored, state);
                }
                let mut retired = old.retired_lifetimes.clone();
                if retired.len() >= 128 {
                    retired.pop_first();
                }
                retired.insert(old.stamp.session_lifetime_id.0.clone());
                ViewState {
                    stamp: stamp.clone(),
                    retired_lifetimes: retired,
                    runs: BTreeMap::new(),
                    interactions: BTreeMap::new(),
                    history_modes: BTreeSet::new(),
                    replay_cursors: BTreeMap::new(),
                }
            } else {
                if !stamp.dominates(&old.stamp)
                    && (baseline
                        || !stamp.owner.as_ref().is_some_and(|owner| {
                            old.runs
                                .get(&owner.run_id.0)
                                .is_some_and(|run| run.owner == *owner && !run.terminal)
                        }))
                {
                    return answer(ViewDecision::Ignored, state);
                }
                old.clone()
            }
        }
    };
    let mode = match &input {
        ViewInput::Readback { snapshot } => match snapshot.history_mode {
            HistoryReadMode::Canonical => "canonical",
            HistoryReadMode::Model => "model",
            HistoryReadMode::Omitted => "omitted",
        }
        .to_owned(),
        ViewInput::Page { page_key, .. } if !page_key.is_empty() && page_key.len() <= 128 => {
            format!("page:{page_key}")
        }
        ViewInput::Page { .. } => return answer(ViewDecision::Resync, state),
        ViewInput::Event { .. } | ViewInput::Replay { .. } => String::new(),
        ViewInput::Run { .. } => return answer(ViewDecision::Resync, state),
    };
    match input {
        ViewInput::Run { .. } => return answer(ViewDecision::Resync, state),
        ViewInput::Readback { snapshot } | ViewInput::Page { snapshot, .. } => {
            let same = state.as_ref().is_some_and(|old| {
                old.stamp.session_lifetime_id == stamp.session_lifetime_id
                    && old.stamp.snapshot_revision == stamp.snapshot_revision
            });
            let duplicate = same && next.history_modes.contains(&mode);
            if !same {
                next.history_modes.clear();
            }
            next.history_modes.insert(mode);
            for item in next.interactions.values_mut() {
                item.pending = false;
            }
            for interaction in &snapshot.pending_interactions {
                if next
                    .interactions
                    .get(&interaction.interaction_id.0)
                    .is_some_and(|old| old.revision > interaction.revision)
                {
                    return answer(ViewDecision::Ignored, state);
                }
                next.interactions.insert(
                    interaction.interaction_id.0.clone(),
                    ViewInteraction {
                        interaction_id: interaction.interaction_id.clone(),
                        revision: interaction.revision,
                        pending: true,
                    },
                );
            }
            for run in snapshot
                .active_runs
                .iter()
                .chain(snapshot.last_durable_terminal.iter())
            {
                let owner = snapshot
                    .run_owners
                    .iter()
                    .find(|owner| owner.run_id == run.run_id)
                    .or_else(|| {
                        snapshot
                            .active_owner
                            .as_ref()
                            .filter(|owner| owner.run_id == run.run_id)
                    });
                if let Some(owner) = owner {
                    if next.runs.get(&run.run_id.0).is_some_and(|old| {
                        old.owner != *owner
                            || old.event_seq > run.last_seq
                            || (old.terminal && !run.status.terminal())
                    }) {
                        return answer(ViewDecision::Ignored, state);
                    }
                    next.runs.insert(
                        run.run_id.0.clone(),
                        ViewRun {
                            owner: owner.clone(),
                            event_seq: run.last_seq,
                            terminal: run.status.terminal(),
                        },
                    );
                }
            }
            let included = snapshot
                .active_runs
                .iter()
                .chain(snapshot.last_durable_terminal.iter())
                .map(|run| run.run_id.0.as_str())
                .collect::<BTreeSet<_>>();
            next.runs.retain(|id, _| included.contains(id.as_str()));
            next.replay_cursors
                .retain(|id, _| included.contains(id.as_str()));
            next.interactions.retain(|_, item| item.pending);
            if duplicate {
                return ViewReduction {
                    state,
                    decision: ViewDecision::Ignored,
                    duplicate: true,
                };
            }
        }
        ViewInput::Event { .. } | ViewInput::Replay { .. } => {
            let (Some(owner), Some(seq)) = (&stamp.owner, stamp.event_seq) else {
                return answer(ViewDecision::Resync, state);
            };
            match next.runs.get(&owner.run_id.0) {
                Some(old)
                    if old.owner != *owner
                        || (old.terminal && !stamp.terminal)
                        || seq <= old.event_seq =>
                {
                    return answer(ViewDecision::Ignored, state);
                }
                Some(old)
                    if stamp.previous_visible_seq.map_or_else(
                        || old.event_seq.0.checked_add(1) != Some(seq.0),
                        |previous| previous > old.event_seq || previous >= seq,
                    ) =>
                {
                    return answer(ViewDecision::Resync, state);
                }
                None if stamp
                    .previous_visible_seq
                    .map_or(seq.0 != 1, |previous| previous != EventSeq(0)) =>
                {
                    return answer(ViewDecision::Resync, state);
                }
                _ => {}
            }
            if let Some(interaction) = &stamp.interaction {
                if next
                    .interactions
                    .get(&interaction.interaction_id.0)
                    .is_some_and(|old| {
                        interaction.revision < old.revision
                            || (interaction.revision == old.revision && interaction != old)
                    })
                {
                    return answer(ViewDecision::Ignored, state);
                }
                next.interactions
                    .insert(interaction.interaction_id.0.clone(), interaction.clone());
            }
            next.replay_cursors.insert(owner.run_id.0.clone(), seq);
            next.runs.insert(
                owner.run_id.0.clone(),
                ViewRun {
                    owner: owner.clone(),
                    event_seq: seq,
                    terminal: stamp.terminal,
                },
            );
        }
    }
    if next.runs.len() > 128 || next.interactions.len() > 256 || next.history_modes.len() > 256 {
        return answer(ViewDecision::Resync, state);
    }
    let mut stamp = stamp;
    if let Some(old) = &state {
        if old.stamp.session_lifetime_id == stamp.session_lifetime_id {
            stamp.snapshot_revision = stamp.snapshot_revision.max(old.stamp.snapshot_revision);
            stamp.metadata_revision = stamp.metadata_revision.max(old.stamp.metadata_revision);
            stamp.transcript_revision =
                stamp.transcript_revision.max(old.stamp.transcript_revision);
            stamp.projection_generation = stamp
                .projection_generation
                .max(old.stamp.projection_generation);
        }
    }
    next.stamp = stamp;
    answer(ViewDecision::Accepted, Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn snapshot(revision: u64, lifetime: &str) -> SessionReadback {
        serde_json::from_value(json!({"schema_version":1,"session_id":"s","session_lifetime_id":lifetime,"snapshot_revision":revision,"metadata_revision":revision,"metadata":{"key":"s","lifetime":lifetime,"deleted":false,"legacy_imported":true,"revision":5,"updated_at_ms":0},"transcript_revision":5,"projection_generation":1,"history_mode":"omitted","omitted":["messages","batch_ranges"],"messages":[],"batch_ranges":[],"active_owner":null,"run_owners":[],"active_runs":[],"queue_rows":[],"queue_cursor":null,"pending_interactions":[],"last_durable_terminal":null,"current_plan":null,"plan_digest":null,"context_usage":null})).unwrap()
    }
    fn baseline(revision: u64, lifetime: &str) -> ViewInput {
        ViewInput::Readback {
            snapshot: Box::new(snapshot(revision, lifetime)),
        }
    }
    fn event(revision: u64, lifetime: &str, run: &str, generation: u64, seq: u64) -> ViewInput {
        let mut stamp = ViewStamp::readback(&snapshot(revision, lifetime));
        stamp.owner = Some(ExactOwner {
            session_key: SessionKey("s".into()),
            session_lifetime_id: SessionLifetimeId(lifetime.into()),
            run_id: RunId(run.into()),
            run_generation: RunGeneration(generation),
            turn_id: TurnId(format!("turn-{run}")),
        });
        stamp.event_seq = Some(EventSeq(seq));
        ViewInput::Event {
            stamp: Some(Box::new(stamp)),
        }
    }
    #[test]
    fn independent_run_events_join_versions_and_exact_turns_are_fenced() {
        let initial = reduce_view(None, baseline(10, "a"));
        let a = reduce_view(initial.state, event(11, "a", "a", 1, 1));
        let b = reduce_view(a.state, event(14, "a", "b", 2, 1));
        let a_late = reduce_view(b.state, event(12, "a", "a", 1, 2));
        assert_eq!(
            a_late.decision,
            ViewDecision::Accepted,
            "另一个 run 的独立前进不能丢掉合法增量"
        );
        assert_eq!(
            a_late.state.as_ref().unwrap().stamp.snapshot_revision,
            SnapshotRevision(14)
        );
        assert_eq!(
            a_late.state.as_ref().unwrap().runs["a"].event_seq,
            EventSeq(2)
        );
        assert_eq!(
            a_late.state.as_ref().unwrap().runs["b"].event_seq,
            EventSeq(1)
        );
        let mut wrong_turn = event(15, "a", "a", 1, 3);
        if let ViewInput::Event { stamp: Some(stamp) } = &mut wrong_turn {
            stamp.owner.as_mut().unwrap().turn_id = TurnId("wrong".into());
        }
        let rejected = reduce_view(a_late.state.clone(), wrong_turn);
        assert_eq!(rejected.decision, ViewDecision::Ignored);
        assert_eq!(rejected.state, a_late.state);
        assert_eq!(
            reduce_view(a_late.state, baseline(13, "a")).decision,
            ViewDecision::Ignored
        );
    }
    #[test]
    fn durable_replay_only_materializes_current_active_owner_and_known_pending_revision() {
        let live = reduce_view(
            reduce_view(None, baseline(10, "a")).state,
            event(11, "a", "r", 1, 1),
        );
        let mut canonical = snapshot(20, "a");
        let owner = live.state.as_ref().unwrap().runs["r"].owner.clone();
        canonical.run_owners = vec![owner.clone()];
        canonical.active_owner = Some(owner.clone());
        canonical.active_runs = vec![RunRecord {
            kind: RunKind::Chat,
            run_id: owner.run_id.clone(),
            turn_id: owner.turn_id.clone(),
            session_id: owner.session_key.clone(),
            request_id: RequestId::Number(1),
            status: RunStatus::Running,
            last_seq: EventSeq(5),
            content: None,
            error_code: None,
            error_message: None,
        }];
        canonical.pending_interactions = vec![InteractionRecord {
            interaction_id: InteractionId("i".into()),
            session_id: owner.session_key.clone(),
            owner_run_id: owner.run_id.clone(),
            kind: "approval".into(),
            status: "pending".into(),
            revision: 2,
            prompt: "确认".into(),
            payload: serde_json::Value::Null,
            response: None,
        }];
        let current = reduce_view(
            live.state,
            ViewInput::Readback {
                snapshot: Box::new(canonical),
            },
        );
        let ViewInput::Event { stamp } = event(12, "a", "r", 1, 2) else {
            unreachable!()
        };
        assert_eq!(
            reduce_view(
                current.state.clone(),
                ViewInput::Event {
                    stamp: stamp.clone()
                }
            )
            .decision,
            ViewDecision::Ignored
        );
        let replay = reduce_view(
            current.state.clone(),
            ViewInput::Replay {
                stamp: stamp.clone(),
                after_seq: EventSeq(0),
            },
        );
        assert_eq!(replay.decision, ViewDecision::Accepted);
        assert_eq!(
            replay.state.as_ref().unwrap().stamp,
            current.state.as_ref().unwrap().stamp,
            "重放只补展示，不能回退版本"
        );
        assert_eq!(
            reduce_view(
                replay.state.clone(),
                ViewInput::Replay {
                    stamp,
                    after_seq: EventSeq(0)
                }
            )
            .decision,
            ViewDecision::Ignored
        );
        let ViewInput::Event { mut stamp } = event(13, "a", "r", 1, 3) else {
            unreachable!()
        };
        stamp.as_mut().unwrap().interaction = Some(ViewInteraction {
            interaction_id: InteractionId("i".into()),
            revision: 1,
            pending: true,
        });
        let old_approval = reduce_view(
            replay.state,
            ViewInput::Replay {
                stamp,
                after_seq: EventSeq(0),
            },
        );
        assert_eq!(old_approval.decision, ViewDecision::Ignored);
        let ViewInput::Event { mut stamp } = event(14, "a", "r", 1, 4) else {
            unreachable!()
        };
        stamp.as_mut().unwrap().interaction = Some(ViewInteraction {
            interaction_id: InteractionId("i".into()),
            revision: 2,
            pending: true,
        });
        assert_eq!(
            reduce_view(
                old_approval.state,
                ViewInput::Replay {
                    stamp,
                    after_seq: EventSeq(0)
                }
            )
            .decision,
            ViewDecision::Accepted
        );
    }
    #[test]
    fn live_readback_order_duplicates_gaps_lifetime_and_multirun_never_regress() {
        assert_eq!(
            reduce_view(None, event(11, "a", "r", 1, 1)).decision,
            ViewDecision::Resync
        );
        let a = reduce_view(None, baseline(10, "a")).state;
        let live = reduce_view(a, event(11, "a", "r", 1, 1));
        assert_eq!(live.decision, ViewDecision::Accepted);
        let stale = reduce_view(live.state.clone(), baseline(10, "a"));
        assert_eq!(stale.decision, ViewDecision::Ignored);
        assert_eq!(stale.state, live.state);
        let duplicate = reduce_view(live.state.clone(), event(11, "a", "r", 1, 1));
        assert_eq!(duplicate.decision, ViewDecision::Ignored);
        assert_eq!(duplicate.state, live.state);
        assert_eq!(
            reduce_view(live.state.clone(), event(12, "a", "r", 1, 3)).decision,
            ViewDecision::Resync
        );
        assert_eq!(
            reduce_view(live.state.clone(), event(12, "a", "r", 2, 2)).decision,
            ViewDecision::Ignored
        );
        let other = reduce_view(live.state, event(12, "a", "other", 2, 1));
        assert_eq!(other.decision, ViewDecision::Accepted);
        let fresh = reduce_view(other.state, baseline(13, "a"));
        assert_eq!(fresh.decision, ViewDecision::Accepted);
        assert_eq!(
            reduce_view(fresh.state.clone(), event(12, "a", "r", 1, 2)).decision,
            ViewDecision::Ignored
        );
        assert_eq!(
            reduce_view(fresh.state.clone(), event(14, "b", "r-new", 1, 1)).decision,
            ViewDecision::Ignored
        );
        let new = reduce_view(fresh.state, baseline(14, "b"));
        assert_eq!(new.decision, ViewDecision::Accepted);
        let retired = reduce_view(new.state.clone(), baseline(100, "a"));
        assert_eq!(retired.decision, ViewDecision::Ignored);
        assert_eq!(retired.state, new.state);
        assert_eq!(
            reduce_view(new.state, ViewInput::Event { stamp: None }).decision,
            ViewDecision::Resync
        );
    }
    #[test]
    fn revision_dimensions_interaction_conflict_and_same_revision_complement_are_explicit() {
        let state = reduce_view(None, baseline(10, "a")).state;
        for dimension in 0..3 {
            let mut newer = snapshot(11, "a");
            match dimension {
                0 => newer.metadata_revision = SnapshotRevision(9),
                1 => newer.transcript_revision = TranscriptSeq(4),
                _ => newer.projection_generation = ProjectionGeneration(0),
            }
            assert_eq!(
                reduce_view(
                    state.clone(),
                    ViewInput::Readback {
                        snapshot: Box::new(newer)
                    }
                )
                .decision,
                ViewDecision::Ignored
            );
        }
        let mut complementary = snapshot(10, "a");
        complementary.history_mode = HistoryReadMode::Canonical;
        let full = reduce_view(
            state.clone(),
            ViewInput::Readback {
                snapshot: Box::new(complementary.clone()),
            },
        );
        assert_eq!(full.decision, ViewDecision::Accepted);
        assert_eq!(
            reduce_view(
                full.state,
                ViewInput::Readback {
                    snapshot: Box::new(complementary)
                }
            )
            .decision,
            ViewDecision::Ignored
        );
        let mut request = event(11, "a", "r", 1, 1);
        if let ViewInput::Event { stamp: Some(stamp) } = &mut request {
            stamp.interaction = Some(ViewInteraction {
                interaction_id: InteractionId("i".into()),
                revision: 2,
                pending: true,
            });
        }
        let waiting = reduce_view(state, request);
        assert_eq!(waiting.decision, ViewDecision::Accepted);
        let mut conflict = event(12, "a", "r", 1, 2);
        if let ViewInput::Event { stamp: Some(stamp) } = &mut conflict {
            stamp.interaction = Some(ViewInteraction {
                interaction_id: InteractionId("i".into()),
                revision: 2,
                pending: false,
            });
        }
        assert_eq!(
            reduce_view(waiting.state, conflict).decision,
            ViewDecision::Ignored
        );
    }
}
