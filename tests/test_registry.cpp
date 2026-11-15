#include "failure_fabric/ff.hpp"
#include "fft.hpp"
#include <random>
#include <string>

using namespace ff;

static AuthorityEnvelope make_auth(std::mt19937_64& rng, uint64_t gen) {
  AuthorityEnvelope a;
  a.coordinator_epoch = CoordinatorEpoch(1);
  a.attempt = AttemptId::generate(rng);
  a.dispatch = DispatchId::generate(rng);
  a.attempt_generation = AttemptGeneration(gen);
  a.operation_generation = OperationGeneration(gen);
  a.worker_boot = WorkerBootId::generate(rng);
  return a;
}

TEST(registry_one_terminal_outcome) {
  std::mt19937_64 rng(1);
  OperationRegistry reg;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  CHECK_EQ(reg.create(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.run(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.report_completion(op, a1, false), ApplyResult::OK);
  CHECK_EQ(reg.confirm_completion(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.state(op), OperationState::COMPLETION_CONFIRMED);
  CHECK_EQ(reg.record_terminal(op, a1, "COMPLETED"), ApplyResult::OK); // idempotent same outcome
  CHECK_EQ(reg.record_terminal(op, a1, "TERMINAL_FAILED"), ApplyResult::ALREADY_TERMINAL); // different outcome rejected
  // cannot mutate terminal
  CHECK_EQ(reg.transition(op, OperationState::RUNNING, a1), ApplyResult::ALREADY_TERMINAL);
  CHECK_EQ(reg.state(op), OperationState::COMPLETION_CONFIRMED);
}

TEST(registry_rejects_stale_boot_completion) {
  std::mt19937_64 rng(2);
  OperationRegistry reg;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  CHECK_EQ(reg.create(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.run(op, a1), ApplyResult::OK);
  // a fresh worker restart takes over with a fresh boot at gen 2
  AuthorityEnvelope a2 = a1;
  a2.worker_boot = WorkerBootId::generate(rng);
  a2.attempt = AttemptId::generate(rng);
  a2.attempt_generation = AttemptGeneration(2);
  a2.operation_generation = OperationGeneration(2);
  CHECK_EQ(reg.dispatch(op, a2), ApplyResult::OK); // advance authority to a2
  // the OLD worker (a1) completion must be rejected as stale
  CHECK_EQ(reg.report_completion(op, a1, false), ApplyResult::STALE_REJECTED);
  // the NEW worker succeds
  CHECK_EQ(reg.run(op, a2), ApplyResult::OK);
  CHECK_EQ(reg.report_completion(op, a2, false), ApplyResult::OK);
  CHECK_EQ(reg.confirm_completion(op, a2), ApplyResult::OK);
  CHECK_EQ(reg.state(op), OperationState::COMPLETION_CONFIRMED);
}

TEST(registry_rejects_old_epoch_and_duplicate_report) {
  std::mt19937_64 rng(3);
  OperationRegistry reg;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  CHECK_EQ(reg.create(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.run(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.report_completion(op, a1, false), ApplyResult::OK);
  CHECK_EQ(reg.report_completion(op, a1, false), ApplyResult::ALREADY_REPORTED);
  CHECK_EQ(reg.confirm_completion(op, a1), ApplyResult::OK);
  // old epoch is rejected
  AuthorityEnvelope old = a1;
  old.coordinator_epoch = CoordinatorEpoch(0);
  CHECK_EQ(reg.record_terminal(op, old, "TERMINAL_FAILED"), ApplyResult::ALREADY_TERMINAL);
}

TEST(idempotency_new_then_replay_and_conflict) {
  IdempotencyStore s;
  std::mt19937_64 rng(4);
  OperationId op = OperationId::generate(rng);
  IdempotencyKey key = IdempotencyKey::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::NEW);
  // same key + hash in flight -> dedupe
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::IN_PROGRESS_REPLAY);
  // conflicting hash -> reject
  CHECK_EQ(s.begin(op, key, 0x9999, a), IdempotencyVerdict::CONFLICT);
  s.complete(op, key, "result=42");
  // completed replay returns prior result without re-execution
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::COMPLETED_REPLAY);
  std::string prior;
  CHECK(s.prior_result(op, key, prior));
  CHECK_EQ(prior, "result=42");
}

TEST(store_append_rejects_duplicate_failure_id) {
  std::mt19937_64 rng(5);
  FailureStore store;
  FailureRecordBuilder b;
  b.failure_class(FailureClass::EXECUTION_FAILURE).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng)).origin(FailureOrigin::WORKER);
  FailureRecord rec = b.build(rng);
  CHECK(store.append_failure(rec));
  CHECK(!store.append_failure(rec)); // duplicate id rejected
  // missing operation/attempt rejected
  FailureRecord bad;
  bad.failure_id = FailureId::generate(rng);
  CHECK(!store.append_failure(bad));
}

TEST(persistence_failure_record_roundtrip) {
  std::mt19937_64 rng(6);
  FailureRecordBuilder b;
  b.failure_class(FailureClass::AMBIGUOUS).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng)).timestamp(1234567890).code(77)
    .summary("worker died after side effect").reason("side_effect=unknown; worker=dead")
    .phase(FailurePhase::EXECUTE).completion(Ambiguity::AMBIGUOUS)
    .side_effect_state(SideEffectState::MAY_OCCURRED).unknown_side_effects(1);
  AuthorityEnvelope a; a.coordinator_epoch = CoordinatorEpoch(2);
  a.attempt = AttemptId::generate(rng); a.attempt_generation = AttemptGeneration(1);
  a.worker_boot = WorkerBootId::generate(rng); a.operation_generation = OperationGeneration(1);
  b.authority(a);
  FailureRecord rec = b.build(rng);
  BinaryWriter w;
  encode_failure_record(w, rec);
  BinaryReader rr(w.buffer().data(), w.buffer().size());
  FailureRecord rec2;
  CHECK(decode_failure_record(rr, rec2));
  CHECK_EQ(rec2.summary, rec.summary);
  CHECK_EQ(rec2.failure_class, rec.failure_class);
  CHECK_EQ(rec2.attempt, rec.attempt);
  CHECK_EQ(rec2.timestamp_ms, rec.timestamp_ms);
  CHECK_EQ(rec2.device, rec.device);
}

TEST(persistence_rejects_corruption_truncation) {
  std::mt19937_64 rng(7);
  FailureRecordBuilder b;
  b.failure_class(FailureClass::TRANSIENT).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng));
  FailureRecord rec = b.build(rng);
  BinaryWriter w;
  encode_failure_record(w, rec);
  std::vector<uint8_t> data = w.buffer();
  // frame then corrupt a byte
  auto frame = encode_frame(7, data.data(), data.size());
  // flip a byte in the payload -> checksum mismatch
  std::vector<uint8_t> bad = frame;
  bad[15] ^= 0xFF;
  FrameHeader h; size_t next = 0;
  CHECK(!decode_frame(bad.data(), bad.size(), 0, h, next));
  // truncation
  std::vector<uint8_t> trunc(frame.begin(), frame.end() - 3);
  CHECK(!decode_frame(trunc.data(), trunc.size(), 0, h, next));
  // unsupported magic
  std::vector<uint8_t> badver = frame;
  badver[0] = 0; // corrupt magic first byte
  CHECK(!decode_frame(badver.data(), badver.size(), 0, h, next));
}

TEST(store_save_load_roundtrip_terminal) {
  std::mt19937_64 rng(8);
  FailureStore store;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  CHECK_EQ(store.ops().create(op, a), ApplyResult::OK);
  CHECK_EQ(store.ops().dispatch(op, a), ApplyResult::OK);
  CHECK_EQ(store.ops().run(op, a), ApplyResult::OK);
  CHECK_EQ(store.ops().report_completion(op, a, false), ApplyResult::OK);
  CHECK_EQ(store.ops().confirm_completion(op, a), ApplyResult::OK);
  FailureRecordBuilder b;
  b.failure_class(FailureClass::PARTIAL_SUCCESS).operation(op).attempt(a.attempt).authority(a);
  store.append_failure(b.build(rng));

  const std::string path = "ff_test_store.bin";
  CHECK(store.save(path));
  FailureStore loaded;
  CHECK(loaded.load(path));
  CHECK_EQ(loaded.ops().state(op), OperationState::COMPLETION_CONFIRMED);
  CHECK(loaded.has_failure(store.failures()[0].failure_id));
  std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// Regression tests
// ---------------------------------------------------------------------------
#include <fstream>

namespace {
std::vector<uint8_t> frame_of(uint8_t kind, const BinaryWriter& w) {
  return encode_frame(kind, w.buffer().data(), w.buffer().size());
}
void put_auth(BinaryWriter& w, const AuthorityEnvelope& e) {
  w.generation(e.coordinator_epoch); w.uuid(e.worker_boot.raw); w.uuid(e.attempt.raw);
  w.uuid(e.dispatch.raw); w.generation(e.attempt_generation); w.generation(e.operation_generation);
  w.generation(e.failure_generation); w.generation(e.recovery_generation);
}
// Snapshot header frame: magic, version and the three record counts.
std::vector<uint8_t> header_frame(uint32_t failures, uint32_t ops, uint32_t idem) {
  BinaryWriter w;
  w.u32(kFFMagic); w.u16(kFFVersion); w.u32(failures); w.u32(ops); w.u32(idem);
  return frame_of(0, w);
}
// Operation frame laid out as FailureStore::save writes it.
std::vector<uint8_t> op_frame(const OperationId& op, uint32_t state, bool terminal,
                              const AuthorityEnvelope& a, bool trailing_byte = false) {
  BinaryWriter w;
  w.uuid(op.raw); w.enumv(state); w.u32(1); w.u8(terminal ? 1 : 0);
  w.str16(terminal ? "COMPLETED" : ""); w.u8(0); w.uuid(uuid128{});
  put_auth(w, a); put_auth(w, terminal ? a : AuthorityEnvelope{});
  if (trailing_byte) w.u8(0xEE);
  return frame_of(2, w);
}
// Idempotency frame laid out as FailureStore::save writes it.
std::vector<uint8_t> idem_frame(const OperationId& op, const IdempotencyKey& key, uint32_t state) {
  BinaryWriter w;
  w.uuid(op.raw); w.uuid(key.raw); w.u64(7); w.enumv(state); w.str16("r");
  return frame_of(3, w);
}
bool write_snapshot(const std::string& path, const std::vector<std::vector<uint8_t>>& frames) {
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  for (const auto& fr : frames) f.write(reinterpret_cast<const char*>(fr.data()), static_cast<std::streamsize>(fr.size()));
  f.close();
  return f.good();
}
const uint32_t kStateRunning = static_cast<uint32_t>(OperationState::RUNNING);
const uint32_t kStateConfirmed = static_cast<uint32_t>(OperationState::COMPLETION_CONFIRMED);
}

// Recovery ownership survives a save/load: after a reload a stale owner cannot
// take recovery back under an old generation.
TEST(store_reload_keeps_recovery_ownership) {
  std::mt19937_64 rng(21);
  FailureStore store;
  AuthorityEnvelope a = make_auth(rng, 1);
  WorkerBootId holder = WorkerBootId::generate(rng);
  CHECK(store.ownership().adopt(RecoveryOwner::COORDINATOR, RecoveryGeneration(1), a.worker_boot, a));
  CHECK(store.ownership().transfer(RecoveryOwner::REPLACEMENT_WORKER, RecoveryGeneration(4), holder, a));
  const std::string path = "ff_test_ownership.bin";
  CHECK(store.save(path));
  FailureStore loaded;
  CHECK(loaded.load(path));
  std::remove(path.c_str());
  CHECK_EQ(loaded.ownership().owner(), RecoveryOwner::REPLACEMENT_WORKER);
  CHECK_EQ(loaded.ownership().generation().value, uint64_t(4));
  CHECK(loaded.ownership().holder_boot() == holder);
  CHECK(!loaded.ownership().transfer(RecoveryOwner::ORIGINAL_WORKER, RecoveryGeneration(2), a.worker_boot, a));
  CHECK_EQ(loaded.ownership().owner(), RecoveryOwner::REPLACEMENT_WORKER);
}

// A rejected load leaves the store untouched, including recovery ownership.
TEST(store_rejected_load_leaves_ownership_untouched) {
  std::mt19937_64 rng(22);
  FailureStore src;
  AuthorityEnvelope a = make_auth(rng, 1);
  CHECK(src.ownership().adopt(RecoveryOwner::OPERATOR, RecoveryGeneration(3), a.worker_boot, a));
  const std::string path = "ff_test_bad_tail.bin";
  CHECK(src.save(path));
  { std::ofstream f(path, std::ios::binary | std::ios::app); f.put('\x01'); f.put('\x02'); }
  FailureStore target;
  const bool loaded = target.load(path);
  std::remove(path.c_str());
  CHECK(!loaded);
  CHECK(target.ownership().is_unassigned());
  CHECK_EQ(target.ownership().generation().value, uint64_t(0));
}

TEST(store_load_rejects_invalid_operation_state) {
  std::mt19937_64 rng(23);
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  const std::string path = "ff_test_crafted_state.bin";
  // The hand-built encoding itself is accepted...
  CHECK(write_snapshot(path, {header_frame(0, 1, 0), op_frame(op, kStateConfirmed, true, a)}));
  FailureStore ok;
  const bool control = ok.load(path);
  // ...an operation state outside the enum is not.
  CHECK(write_snapshot(path, {header_frame(0, 1, 0), op_frame(op, 20, false, a)}));
  FailureStore bad;
  const bool invalid = bad.load(path);
  std::remove(path.c_str());
  CHECK(control);
  CHECK_EQ(ok.ops().state(op), OperationState::COMPLETION_CONFIRMED);
  CHECK(!invalid);
  CHECK_EQ(bad.ops().size(), size_t(0));
}

TEST(store_load_rejects_terminal_outcome_on_running_operation) {
  std::mt19937_64 rng(24);
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  const std::string path = "ff_test_crafted_terminal.bin";
  CHECK(write_snapshot(path, {header_frame(0, 1, 0), op_frame(op, kStateRunning, true, a)}));
  FailureStore bad;
  const bool loaded = bad.load(path);
  std::remove(path.c_str());
  CHECK(!loaded);
}

TEST(store_load_rejects_trailing_bytes_in_frame) {
  std::mt19937_64 rng(25);
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  const std::string path = "ff_test_crafted_trailing.bin";
  CHECK(write_snapshot(path, {header_frame(0, 1, 0), op_frame(op, kStateConfirmed, true, a, true)}));
  FailureStore bad;
  const bool loaded = bad.load(path);
  std::remove(path.c_str());
  CHECK(!loaded);
}

TEST(store_load_rejects_invalid_and_duplicate_idempotency_entries) {
  std::mt19937_64 rng(26);
  OperationId op = OperationId::generate(rng);
  IdempotencyKey key = IdempotencyKey::generate(rng);
  const std::string path = "ff_test_crafted_idem.bin";
  CHECK(write_snapshot(path, {header_frame(0, 0, 1), idem_frame(op, key, 1)}));
  FailureStore ok;
  const bool control = ok.load(path);
  CHECK(write_snapshot(path, {header_frame(0, 0, 1), idem_frame(op, key, 4)}));
  FailureStore bad_state;
  const bool invalid_state = bad_state.load(path);
  CHECK(write_snapshot(path, {header_frame(0, 0, 2), idem_frame(op, key, 1), idem_frame(op, key, 1)}));
  FailureStore dup;
  const bool duplicate = dup.load(path);
  std::remove(path.c_str());
  CHECK(control);
  CHECK_EQ(ok.idem().size(), size_t(1));
  CHECK(!invalid_state);
  CHECK(!duplicate);
}

// The failure log only admits records the snapshot codec can round-trip.
TEST(store_append_rejects_unpersistable_record) {
  std::mt19937_64 rng(27);
  FailureStore store;
  FailureRecordBuilder longer;
  longer.failure_class(FailureClass::EXECUTION_FAILURE).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng)).summary(std::string(70000, 'x'));
  CHECK(!store.append_failure(longer.build(rng)));
  FailureRecordBuilder reason;
  reason.failure_class(FailureClass::EXECUTION_FAILURE).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng)).reason(std::string(65536, 'x'));
  CHECK(!store.append_failure(reason.build(rng)));
  FailureRecordBuilder deps;
  deps.failure_class(FailureClass::DEPENDENCY_FAILURE).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng));
  for (int i = 0; i < 4097; ++i) deps.dependency(FailureId::generate(rng));
  CHECK(!store.append_failure(deps.build(rng)));
  CHECK_EQ(store.failure_count(), size_t(0));

  // A record at the limits is accepted and survives a reload intact.
  FailureRecordBuilder ok;
  ok.failure_class(FailureClass::DEPENDENCY_FAILURE).operation(OperationId::generate(rng))
    .attempt(AttemptId::generate(rng)).summary(std::string(65535, 'y'));
  for (int i = 0; i < 4096; ++i) ok.dependency(FailureId::generate(rng));
  FailureRecord rec = ok.build(rng);
  CHECK(store.append_failure(rec));
  const std::string path = "ff_test_limits.bin";
  CHECK(store.save(path));
  FailureStore loaded;
  const bool reloaded = loaded.load(path);
  std::remove(path.c_str());
  CHECK(reloaded);
  const FailureRecord* back = loaded.find_failure(rec.failure_id);
  CHECK(back != nullptr);
  CHECK_EQ(back->summary.size(), size_t(65535));
  CHECK_EQ(back->dependencies.size(), size_t(4096));
}

// A prior result too long for the codec is never silently saved as empty.
TEST(store_save_refuses_unrepresentable_idempotency_result) {
  std::mt19937_64 rng(28);
  FailureStore store;
  OperationId op = OperationId::generate(rng);
  IdempotencyKey key = IdempotencyKey::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  CHECK_EQ(store.idem().begin(op, key, 1, a), IdempotencyVerdict::NEW);
  store.idem().complete(op, key, std::string(70000, 'r'));
  const std::string path = "ff_test_idem_result.bin";
  const bool saved_oversize = store.save(path);
  store.idem().complete(op, key, std::string(65535, 'r'));
  const bool saved = store.save(path);
  FailureStore loaded;
  const bool reloaded = loaded.load(path);
  std::remove(path.c_str());
  CHECK(!saved_oversize);
  CHECK(saved);
  CHECK(reloaded);
  std::string prior;
  CHECK(loaded.idem().prior_result(op, key, prior));
  CHECK_EQ(prior.size(), size_t(65535));
}

// A replayed dispatch of a superseded attempt must not take authority back.
TEST(registry_rejects_stale_dispatch) {
  std::mt19937_64 rng(29);
  OperationRegistry reg;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  CHECK_EQ(reg.create(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.run(op, a1), ApplyResult::OK);
  AuthorityEnvelope a2 = make_auth(rng, 2);
  CHECK_EQ(reg.dispatch(op, a2), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::STALE_REJECTED);
  CHECK(reg.current_authority(op).attempt == a2.attempt);
  CHECK_EQ(reg.find(op)->attempt_count, uint32_t(2));
  CHECK_EQ(reg.report_completion(op, a1, false), ApplyResult::STALE_REJECTED);
  AuthorityEnvelope old_epoch = make_auth(rng, 3);
  old_epoch.coordinator_epoch = CoordinatorEpoch(0);
  CHECK_EQ(reg.dispatch(op, old_epoch), ApplyResult::STALE_REJECTED);
  CHECK(reg.current_authority(op).attempt == a2.attempt);
  // the current attempt still completes
  CHECK_EQ(reg.run(op, a2), ApplyResult::OK);
  CHECK_EQ(reg.report_completion(op, a2, false), ApplyResult::OK);
  CHECK_EQ(reg.confirm_completion(op, a2), ApplyResult::OK);
}

// A dispatch that is refused leaves the current authority untouched.
TEST(registry_rejected_dispatch_keeps_authority) {
  std::mt19937_64 rng(30);
  OperationRegistry reg;
  OperationId op = OperationId::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  CHECK_EQ(reg.create(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(op, a1), ApplyResult::OK);
  CHECK_EQ(reg.transition(op, OperationState::CANCELLED, a1), ApplyResult::OK);
  AuthorityEnvelope a2 = make_auth(rng, 2);
  CHECK_EQ(reg.dispatch(op, a2), ApplyResult::ILLEGAL_TRANSITION);
  CHECK(reg.current_authority(op).attempt == a1.attempt);
  CHECK_EQ(reg.state(op), OperationState::CANCELLED);
}

// The terminal state recorded is the one the outcome names.
TEST(registry_terminal_state_follows_outcome) {
  std::mt19937_64 rng(31);
  OperationRegistry reg;
  AuthorityEnvelope a = make_auth(rng, 1);

  OperationId recovered = OperationId::generate(rng);
  CHECK_EQ(reg.create(recovered, a), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(recovered, a), ApplyResult::OK);
  CHECK_EQ(reg.run(recovered, a), ApplyResult::OK);
  CHECK_EQ(reg.report_failure(recovered, a, false), ApplyResult::OK);
  CHECK_EQ(reg.transition(recovered, OperationState::RECOVERY_PENDING, a), ApplyResult::OK);
  CHECK_EQ(reg.transition(recovered, OperationState::RECOVERING, a), ApplyResult::OK);
  CHECK_EQ(reg.record_terminal(recovered, a, "RECOVERED"), ApplyResult::OK);
  CHECK_EQ(reg.state(recovered), OperationState::RECOVERED);

  OperationId abandoned = OperationId::generate(rng);
  CHECK_EQ(reg.create(abandoned, a), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(abandoned, a), ApplyResult::OK);
  CHECK_EQ(reg.run(abandoned, a), ApplyResult::OK);
  CHECK_EQ(reg.report_failure(abandoned, a, true), ApplyResult::OK);
  CHECK_EQ(reg.record_terminal(abandoned, a, "ABANDONED"), ApplyResult::OK);
  CHECK_EQ(reg.state(abandoned), OperationState::ABANDONED);

  // An outcome whose state is not reachable is refused and nothing is recorded.
  OperationId running = OperationId::generate(rng);
  CHECK_EQ(reg.create(running, a), ApplyResult::OK);
  CHECK_EQ(reg.dispatch(running, a), ApplyResult::OK);
  CHECK_EQ(reg.run(running, a), ApplyResult::OK);
  CHECK_EQ(reg.record_terminal(running, a, "RECOVERED"), ApplyResult::ILLEGAL_TRANSITION);
  CHECK_EQ(reg.state(running), OperationState::RUNNING);
  CHECK(!reg.find(running)->terminal_recorded);
}

// After a failed execution exactly one retry is NEW; duplicates are deduplicated.
TEST(idempotency_failed_retry_is_single_flight) {
  IdempotencyStore s;
  std::mt19937_64 rng(32);
  OperationId op = OperationId::generate(rng);
  IdempotencyKey key = IdempotencyKey::generate(rng);
  AuthorityEnvelope a = make_auth(rng, 1);
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::NEW);
  s.fail(op, key);
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::NEW);
  CHECK_EQ(s.begin(op, key, 0x1234, a), IdempotencyVerdict::IN_PROGRESS_REPLAY);
  CHECK(s.state(op, key) == IdempotencyState::IN_PROGRESS);
}

// A stale generation under a valid key is rejected.
TEST(idempotency_rejects_stale_generation) {
  IdempotencyStore s;
  std::mt19937_64 rng(33);
  OperationId op = OperationId::generate(rng);
  IdempotencyKey key = IdempotencyKey::generate(rng);
  AuthorityEnvelope a1 = make_auth(rng, 1);
  AuthorityEnvelope a2 = make_auth(rng, 2);
  CHECK_EQ(s.begin(op, key, 0x1234, a1), IdempotencyVerdict::NEW);
  s.fail(op, key);
  CHECK_EQ(s.begin(op, key, 0x1234, a2), IdempotencyVerdict::NEW);       // retry under a fresh generation
  CHECK_EQ(s.begin(op, key, 0x1234, a1), IdempotencyVerdict::STALE);     // late duplicate of the old attempt
  CHECK_EQ(s.begin(op, key, 0x1234, a2), IdempotencyVerdict::IN_PROGRESS_REPLAY);
  s.complete(op, key, "result=42");
  CHECK_EQ(s.begin(op, key, 0x1234, a1), IdempotencyVerdict::STALE);
  CHECK_EQ(s.begin(op, key, 0x1234, a2), IdempotencyVerdict::COMPLETED_REPLAY);
  AuthorityEnvelope old_epoch = make_auth(rng, 3);
  old_epoch.coordinator_epoch = CoordinatorEpoch(0);
  CHECK_EQ(s.begin(op, key, 0x1234, old_epoch), IdempotencyVerdict::STALE);
}

int main() { return fft::run_all(); }