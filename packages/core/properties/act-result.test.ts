import { expect, spyOn, test } from "bun:test"
import { Context, Schema } from "effect"
import { act, atom, createEventLog, createStore, effectAtom, EventLog, type ActState, type ActFailure, type EffectRef, type JournalEvent } from "../src/index"
import { LogView } from "../src/runtime/log-view"

const Tick = Schema.Struct({ type: Schema.Literal("Tick"), value: Schema.Finite })
const Job = act({ name: "test.result", input: Schema.Finite, success: Schema.Finite, failure: Schema.String })

// resultRecovery compares indexed replay and log-only projections through pending, terminal, deferred, and cancelled work.
test("act results use indexed absence and preserve replay and log-only results", () => {
  const open = () => {
    const handles = Array.from({ length: 64 }, (_, input) => Job.request({ input }))
    const atoms = Object.fromEntries(handles.map((handle, index) => [String(index), effectAtom(get => ({ view: get(handle.result), events: {}, acts: { job: handle } }))]))
    return { handles, log: createEventLog({ schema: Tick, atoms }) }
  }
  const { handles, log } = open()
  const scans = spyOn(LogView.prototype, "find")
  const refs: EffectRef[] = []
  const expected: ActState<number, ActFailure<string>>[] = handles.map(() => ({ status: "pending" }))
  let snapshot = log.initial
  const initial = snapshot
  const advance = (event: JournalEvent<typeof Tick.Type>) => { snapshot = log.append(snapshot, event) }
  try {
    for (const [index, handle] of handles.entries()) {
      const offered = snapshot.effects().find(work => !work.ref && work.request.input === index)!
      const ref = { seq: snapshot.position, atom: offered.atom, act: Job.name }
      refs.push(ref)
      advance(log.requestEvent(ref, offered.request))
      expect(snapshot.get(handle.result)).toEqual({ status: "pending" })
      const mode = index % 6
      if (mode !== 0) {
        if (mode === 5) {
          advance({ type: "EffectCancelled", ref, reason: "stop" })
          expected[index] = { status: "rejected", reason: { _tag: "Cancelled", reason: "stop" } }
        } else {
          advance({ type: "EffectSettled", ref, outcome: mode === 2 ? { status: "rejected", reason: "failed" } : { status: "fulfilled", value: mode >= 3 ? { type: "promise", handle: { executor: "test", id: String(index) } } : { type: "value", value: index } } })
          if (mode === 1) expected[index] = { status: "fulfilled", value: index }
          if (mode === 2) expected[index] = { status: "rejected", reason: "failed" }
          if (mode === 4) {
            advance({ type: "PromiseSettled", ref, result: { status: "fulfilled", value: index } })
            expected[index] = { status: "fulfilled", value: index }
          }
        }
      }
      advance({ type: "Tick", value: index })
      expect(handles.map(handle => snapshot.get(handle.result))).toEqual(expected)
    }
    expect(handles.map(handle => initial.get(handle.result))).toEqual(handles.map(() => ({ status: "pending" })))
    const restored = open()
    try {
      const replayed = restored.log.replay(snapshot.records)
      expect(restored.handles.map(handle => replayed.get(handle.result))).toEqual(expected)
      expect(replayed.pending().length).toBe(snapshot.pending().length)
      expect(replayed.deferred().length).toBe(snapshot.deferred().length)
      expect(scans.mock.calls.length).toBe(0)
    } finally { restored.log.dispose() }
    scans.mockRestore()
    const legacy = createStore(Context.make(EventLog, { events: atom(snapshot.events), bindings: atom(new Map(handles.map((handle, index) => [handle.identity, refs[index]!])))}))
    try { expect(handles.map(handle => legacy.get(handle.result))).toEqual(expected) }
    finally { legacy.dispose() }
  } finally { scans.mockRestore(); log.dispose() }
})
