// Real multiprocess failure/restart proof: coordinator + 2 worker OS processes,
// real worker death via TerminateProcess, stale-completion replay rejection,
// recovery ownership transfer, and deterministic replay after reload.
// Copyright 2026 Summon Software Labs. Apache-2.0.
#include "failure_fabric/ff.hpp"
#include "failure_fabric/protocol.hpp"
#include "fft.hpp"
#include <windows.h>
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <random>

using namespace ff;

static std::string exe_dir() {
  char buf[MAX_PATH];
  GetModuleFileNameA(nullptr, buf, MAX_PATH);
  std::string p(buf);
  auto pos = p.find_last_of("\\/");
  return pos == std::string::npos ? "." : p.substr(0, pos);
}
static std::string join(const std::string& a, const std::string& b) { return a + "\\" + b; }

static HANDLE spawn(const std::string& exe, const std::string& args) {
  std::string cmd = "\"" + exe + "\" " + args;
  STARTUPINFOA si; memset(&si, 0, sizeof(si)); si.cb = sizeof(si);
  PROCESS_INFORMATION pi; memset(&pi, 0, sizeof(pi));
  std::vector<char> cmdline(cmd.begin(), cmd.end()); cmdline.push_back('\0');
  if (!CreateProcessA(nullptr, cmdline.data(), nullptr, nullptr, FALSE,
                      CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi)) {
    std::printf("  spawn failed: %s err=%lu cmd=[%s]\n", exe.c_str(), (unsigned long)GetLastError(), cmd.c_str());
    return nullptr;
  }
  return pi.hProcess;
}
static void kill(HANDLE h) { if (h && h != INVALID_HANDLE_VALUE) TerminateProcess(h, 21); }
static void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

TEST(multiprocess_worker_death_ambiguous_recovery) {
  const std::string dir = exe_dir();
  std::string workdir = dir + "\\ff_mp_work";
  CreateDirectoryA(workdir.c_str(), nullptr);
  std::string cmd = "cmd /c del /q \"" + workdir + "\\*marker*\" 2>nul";
  system(cmd.c_str());

  uint16_t port = 57321;
  std::string statefile = workdir + "\\state.bin";
  DeleteFileA(statefile.c_str());

  HANDLE coord = spawn(join(dir, "ff_coordinator.exe"), "--port " + std::to_string(port) + " --state \"" + statefile + "\"");
  CHECK(coord != nullptr);
  sleep_ms(800);

  HANDLE workerA = spawn(join(dir, "ff_worker.exe"), "127.0.0.1 " + std::to_string(port) + " \"" + workdir + "\" slow");
  CHECK(workerA != nullptr);
  sleep_ms(800);
  // A third worker registers and disconnects again before any work is
  // dispatched: it must not be picked as the replacement later.
  bool idle_registered = false;
  {
    TcpSocket idle;
    WireMessage reg;
    reg.type = ProtocolMessage::REGISTER;
    reg.worker = WorkerId::generate();
    reg.boot = WorkerBootId::generate();
    WireMessage ack;
    idle_registered = idle.connect_to("127.0.0.1", port) && idle.send(reg) && idle.recv(ack) &&
                      ack.type == ProtocolMessage::REGISTER_ACK;
  }
  sleep_ms(300);
  HANDLE workerB = spawn(join(dir, "ff_worker.exe"), "127.0.0.1 " + std::to_string(port) + " \"" + workdir + "\" normal");
  CHECK(workerB != nullptr);
  sleep_ms(800);

  TcpSocket ctrl;
  CHECK(ctrl.connect_to("127.0.0.1", port));
  WireMessage disp;
  disp.type = ProtocolMessage::DISPATCH;
  disp.op = OperationId::generate();
  disp.text = "O1";
  CHECK(ctrl.send(disp));

  sleep_ms(1200);
  kill(workerA);
  std::printf("  killed worker A while authority was live\n");

  sleep_ms(3500);

  WireMessage stale;
  stale.type = ProtocolMessage::COMPLETE;
  stale.op = disp.op;
  stale.attempt = AttemptId::generate();
  stale.authority.coordinator_epoch = CoordinatorEpoch(1);
  stale.authority.attempt_generation = AttemptGeneration(1);
  stale.authority.operation_generation = OperationGeneration(1);
  stale.authority.attempt = stale.attempt;
  stale.worker = WorkerId::generate();
  stale.boot = WorkerBootId::generate();
  WireMessage resp;
  CHECK(ctrl.send(stale));
  CHECK(ctrl.recv(resp));
  std::printf("  stale completion replay -> coordinator reply type %d (FENCE=%d)\n", static_cast<int>(resp.type), static_cast<int>(ProtocolMessage::FENCE));
  CHECK(resp.type == ProtocolMessage::FENCE);

  sleep_ms(300);
  kill(coord);
  sleep_ms(300);

  HANDLE coord2 = spawn(join(dir, "ff_coordinator.exe"), "--port " + std::to_string(port + 1) + " --state \"" + statefile + "\"");
  CHECK(coord2 != nullptr);
  sleep_ms(500);

  kill(workerB);
  kill(coord2);
  sleep_ms(200);

  // Every process is gone. The state the coordinator persisted must show the
  // whole recovery: the dispatch went to the first registered worker (A), its
  // death was recorded as one ambiguous failure, ownership moved to the
  // replacement, and the replacement's fresh attempt is the one terminal outcome.
  CHECK(idle_registered);
  FailureStore persisted;
  const bool reloaded = persisted.load(statefile);
  const OperationRuntime* rt = reloaded ? persisted.ops().find(disp.op) : nullptr;
  const std::vector<FailureRecord> failures = persisted.failures();
  const bool marker1 = GetFileAttributesA((workdir + "\\attempt_1_marker.txt").c_str()) != INVALID_FILE_ATTRIBUTES;
  const bool marker2 = GetFileAttributesA((workdir + "\\attempt_2_marker.txt").c_str()) != INVALID_FILE_ATTRIBUTES;
  std::printf("  persisted: reloaded=%d op_known=%d state=%s attempts=%u terminal=%d terminal_gen=%llu failures=%zu owner=%s owner_gen=%llu markers=%d/%d\n",
              reloaded ? 1 : 0, rt ? 1 : 0, rt ? to_string(rt->machine.state()) : "-", rt ? rt->attempt_count : 0u,
              rt && rt->terminal_recorded ? 1 : 0,
              static_cast<unsigned long long>(rt ? rt->terminal.attempt_generation.value : 0),
              failures.size(), to_string(persisted.ownership().owner()),
              static_cast<unsigned long long>(persisted.ownership().generation().value), marker1 ? 1 : 0, marker2 ? 1 : 0);
  if (rt && failures.size() == 1) {
    std::printf("  persisted: failure class=%s completion=%s lost_boot_is_terminal_boot=%d owner_boot_is_terminal_boot=%d\n",
                to_string(failures[0].failure_class), to_string(failures[0].completion),
                failures[0].authority.worker_boot == rt->terminal.worker_boot ? 1 : 0,
                persisted.ownership().holder_boot() == rt->terminal.worker_boot ? 1 : 0);
  }
  // the side effect of each attempt really happened
  CHECK(marker1);
  CHECK(marker2);
  CHECK(reloaded);
  CHECK(rt != nullptr);
  CHECK_EQ(rt->machine.state(), OperationState::COMPLETION_CONFIRMED);
  CHECK(rt->terminal_recorded);
  CHECK_EQ(rt->attempt_count, uint32_t(2));
  CHECK_EQ(rt->terminal.attempt_generation.value, uint64_t(2));
  CHECK_EQ(failures.size(), size_t(1));
  CHECK(failures[0].operation == disp.op);
  CHECK_EQ(failures[0].failure_class, FailureClass::AMBIGUOUS);
  CHECK_EQ(failures[0].completion, Ambiguity::AMBIGUOUS);
  // the fresh attempt is bound to the replacement's boot, not the lost worker's
  CHECK(!rt->terminal.worker_boot.is_null());
  CHECK(failures[0].authority.worker_boot != rt->terminal.worker_boot);
  // recovery ownership went to the replacement and survived the reload
  CHECK_EQ(persisted.ownership().owner(), RecoveryOwner::REPLACEMENT_WORKER);
  CHECK_EQ(persisted.ownership().generation().value, uint64_t(1));
  CHECK(persisted.ownership().holder_boot() == rt->terminal.worker_boot);
  std::printf("  [multiprocess] PASS: worker death -> ambiguous -> fenced stale replay -> persisted+reloaded\n");
}

int main() { return fft::run_all(); }
