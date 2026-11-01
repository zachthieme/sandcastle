import { createRequire } from 'node:module';
import { NodeContext_exports, NodeFileSystem_exports, formatErrorMessage } from './chunk-I3ADIJBI.js';
import { Context_exports, CwdError, SyncOutPathError, Effect_exports, resolveCwd, resolveStateDir, DEFAULT_STATE_DIR, getCurrentBranch, generateTempBranchName, Layer_exports, FileDisplay, ClackDisplay, WorktreeDockerSandboxFactory, SandboxConfig, Display, pruneStale, create, copyToWorktree, runHostHooks, startSandbox, resolveGitMounts, patchGitMountsForWindows, SANDBOX_REPO_DIR, remove, removeMountTempDirs, makeSandboxFromHandle, syncOut, withSandboxLifecycle, hasUncommittedChanges, ForkNotSupportedError, registerShutdown, deleteTempBranchIfUnused, reuseSandboxFactoryLayer, PromptError, FileSystem_exports, SandboxFactory, Runtime_exports, SessionCaptureError, Clock_exports, Duration_exports, Option_exports, PromptExpansionTimeoutError, Deferred_exports, AgentError, Ref_exports, SilentDisplay, AgentIdleTimeoutError, Fiber_exports } from './chunk-HTWVYITO.js';
export { createBindMountSandboxProvider, createIsolatedSandboxProvider } from './chunk-LS6Q7MDJ.js';
import { noSandbox } from './chunk-Q4VEZE6P.js';
import './chunk-6NQPCXSG.js';
import { mkdirSync, appendFileSync, openSync, fstatSync, readSync, closeSync } from 'fs';
import path, { join, posix, dirname, relative } from 'path';
import { styleText } from 'util';
import * as clack from '@clack/prompts';
import { readdir, readFile, mkdir, writeFile, access, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { createHash } from 'crypto';

createRequire(import.meta.url);

// src/resumePrecheck.ts
var assertForkSupported = (provider) => {
  if (provider.sessionStorage?.canFork === false) {
    throw new ForkNotSupportedError({
      message: `${provider.name} cannot fork a session: its CLI resumes but has no fork. Use resume() to continue the session in place.`,
      agentProvider: provider.name
    });
  }
};
var assertResumeSessionExists = async (params) => {
  const { provider, sandboxTag, hostRepoDir, resumeSession } = params;
  if (!provider.sessionStorage) {
    throw new Error(`${provider.name} does not support resumeSession`);
  }
  if (sandboxTag === "none") {
    const found = await provider.sessionStorage.findByIdOnHost(resumeSession);
    if (found.path === void 0) {
      throw new Error(
        `resumeSession "${resumeSession}" not found under ${found.searchedRoot}`
      );
    }
    return;
  }
  const exists = await provider.sessionStorage.existsOnHost(
    hostRepoDir,
    resumeSession
  );
  if (!exists) {
    const sessionPath = provider.sessionStorage.hostSessionFilePath(
      hostRepoDir,
      resumeSession
    );
    throw new Error(
      sessionPath === void 0 ? `resumeSession "${resumeSession}" not found` : `resumeSession "${resumeSession}" not found: expected session file at ${sessionPath}`
    );
  }
};

// src/AgentStreamEmitter.ts
var AgentStreamEmitter = class extends Context_exports.Tag(
  "@ai-hero/sandcastle/AgentStreamEmitter"
)() {
};
var agentStreamEmitterLayer = (onEvent) => Layer_exports.succeed(AgentStreamEmitter, {
  emit: onEvent ? (event) => Effect_exports.sync(() => {
    try {
      onEvent(event);
    } catch {
    }
  }) : () => Effect_exports.void
});

// src/PromptPreprocessor.ts
var PROMPT_EXPANSION_TIMEOUT_MS = 3e4;
var SHELL_BLOCK_MARKER = "";
var MARKED_SHELL_BLOCK_PATTERN = new RegExp(
  `!${SHELL_BLOCK_MARKER}\`([^\`]+)\``,
  "gu"
);
var preprocessPrompt = (prompt, sandbox, cwd) => {
  const blocks = [...prompt.matchAll(MARKED_SHELL_BLOCK_PATTERN)].map(
    (match) => ({
      text: match[0],
      command: match[1] ?? "",
      index: match.index
    })
  );
  if (blocks.length === 0) {
    return Effect_exports.succeed(prompt.replaceAll(SHELL_BLOCK_MARKER, ""));
  }
  return Effect_exports.gen(function* () {
    const display = yield* Display;
    return yield* display.taskLog(
      "Expanding shell expressions",
      (message) => Effect_exports.gen(function* () {
        const expanded = yield* Effect_exports.all(
          blocks.map((block) => {
            const { command } = block;
            return Effect_exports.gen(function* () {
              const start = yield* Clock_exports.currentTimeMillis;
              const maybeResult = yield* sandbox.exec(command, { cwd }).pipe(
                Effect_exports.timeoutOption(
                  Duration_exports.millis(PROMPT_EXPANSION_TIMEOUT_MS)
                )
              );
              if (Option_exports.isNone(maybeResult)) {
                const elapsedMs = (yield* Clock_exports.currentTimeMillis) - start;
                return yield* new PromptExpansionTimeoutError({
                  message: `Shell expression \`${command}\` timed out after ${elapsedMs}ms`,
                  timeoutMs: PROMPT_EXPANSION_TIMEOUT_MS,
                  expression: command,
                  elapsedMs
                });
              }
              const execResult = maybeResult.value;
              if (execResult.exitCode !== 0) {
                return yield* new PromptError({
                  message: `Command \`${command}\` exited with code ${execResult.exitCode}: ${execResult.stderr}`,
                  exitCode: execResult.exitCode
                });
              }
              return { ...block, output: execResult.stdout.trimEnd() };
            });
          }),
          { concurrency: "unbounded" }
        );
        for (const { command, output } of expanded) {
          const tokens = Math.ceil(output.length / 4);
          message(`${command} \u2192 ~${tokens} tokens`);
        }
        let result = "";
        let cursor2 = 0;
        for (const { index, text: text2, output } of expanded) {
          result += prompt.slice(cursor2, index) + output;
          cursor2 = index + text2.length;
        }
        result += prompt.slice(cursor2);
        return result.replaceAll(SHELL_BLOCK_MARKER, "");
      })
    );
  });
};

// src/TextDeltaBuffer.ts
var LENGTH_THRESHOLD = 80;
var DEBOUNCE_MS = 50;
var SENTENCE_BOUNDARY_RE = /[.!?] $/u;
var TextDeltaBuffer = class {
  buffer = "";
  timer = null;
  onFlush;
  constructor(onFlush) {
    this.onFlush = onFlush;
  }
  write(text2) {
    if (text2.length === 0) {
      return;
    }
    this.buffer += text2;
    this.clearTimer();
    if (this.shouldFlush()) {
      this.doFlush();
      return;
    }
    this.timer = setTimeout(() => {
      this.doFlush();
    }, DEBOUNCE_MS);
  }
  /** Force-flush any buffered text. */
  flush() {
    this.clearTimer();
    this.doFlush();
  }
  /** Flush remaining buffer and clean up. */
  dispose() {
    this.flush();
  }
  shouldFlush() {
    if (this.buffer.includes("\n")) {
      return true;
    }
    if (SENTENCE_BOUNDARY_RE.test(this.buffer)) {
      return true;
    }
    if (this.buffer.length >= LENGTH_THRESHOLD) {
      return true;
    }
    return false;
  }
  doFlush() {
    if (this.buffer.length === 0) {
      return;
    }
    const text2 = this.buffer;
    this.buffer = "";
    this.onFlush(text2);
  }
  clearTimer() {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
};

// src/ContextWindow.ts
var formatContextWindow = (contextWindow) => {
  const used = `${Math.ceil(contextWindow.tokens / 1e3)}k`;
  return contextWindow.limit === void 0 ? used : `${used} / ${Math.floor(contextWindow.limit / 1e3)}k`;
};
var buildContextWindowLines = (iterations) => iterations.flatMap(
  (it) => it.contextWindow ? [`Context window: ${formatContextWindow(it.contextWindow)}`] : []
);

// src/Orchestrator.ts
var IDLE_WARNING_INTERVAL_MS = 6e4;
var invokeAgent = (sandbox, sandboxRepoDir, prompt, provider, idleTimeoutMs, completionTimeoutMs, completionSignals, onText, onToolCall, onRawLine, onIdleWarning, onCompletionTimeout, onContextWindow, idleWarningIntervalMs = IDLE_WARNING_INTERVAL_MS, resumeSession, forkSession, signal) => Effect_exports.gen(function* () {
  let resultText = "";
  let streamError;
  let sessionId;
  let usage;
  let accumulatedOutput = "";
  const timeoutSignal = yield* Deferred_exports.make();
  const completionTimeoutDeferred = yield* Deferred_exports.make();
  const runFork = Runtime_exports.runFork(yield* Effect_exports.runtime());
  let timeoutFiber = null;
  let completionDetected = false;
  let warningFiber = null;
  let idleMinuteCounter = 0;
  let settled = false;
  const interruptFiber = (fiber) => {
    if (fiber !== null) {
      runFork(Fiber_exports.interrupt(fiber));
    }
  };
  const startWarningInterval = () => {
    interruptFiber(warningFiber);
    idleMinuteCounter = 0;
    warningFiber = runFork(
      Effect_exports.gen(function* () {
        while (true) {
          yield* Effect_exports.sleep(Duration_exports.millis(idleWarningIntervalMs));
          idleMinuteCounter++;
          onIdleWarning(idleMinuteCounter);
        }
      })
    );
  };
  const resetTimer = () => {
    interruptFiber(timeoutFiber);
    if (completionDetected) {
      timeoutFiber = runFork(
        Effect_exports.gen(function* () {
          yield* Effect_exports.sleep(Duration_exports.millis(completionTimeoutMs));
          onCompletionTimeout(completionTimeoutMs);
          yield* Deferred_exports.succeed(completionTimeoutDeferred, {
            result: resultText || accumulatedOutput,
            sessionId,
            usage
          });
        })
      );
    } else {
      timeoutFiber = runFork(
        Effect_exports.gen(function* () {
          yield* Effect_exports.sleep(Duration_exports.millis(idleTimeoutMs));
          yield* Deferred_exports.fail(
            timeoutSignal,
            new AgentIdleTimeoutError({
              message: `Agent idle for ${idleTimeoutMs / 1e3} seconds \u2014 no output received. Increase the idleTimeoutSeconds option to allow longer silences.`,
              timeoutMs: idleTimeoutMs
            })
          );
        })
      );
      startWarningInterval();
    }
  };
  const abortDeferred = yield* Deferred_exports.make();
  let abortCleanup = null;
  if (signal) {
    if (signal.aborted) {
      return yield* Effect_exports.die(signal.reason);
    }
    const onAbort = () => {
      runFork(Deferred_exports.die(abortDeferred, signal.reason));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    abortCleanup = () => {
      signal.removeEventListener("abort", onAbort);
    };
  }
  resetTimer();
  const execEffect = Effect_exports.gen(function* () {
    const printCmd = provider.buildPrintCommand({
      prompt,
      dangerouslySkipPermissions: true,
      resumeSession,
      forkSession
    });
    const execResult = yield* sandbox.exec(printCmd.command, {
      onLine: (line) => {
        if (settled) {
          return;
        }
        try {
          onRawLine(line);
        } catch {
        }
        for (const parsed of provider.parseStreamLine(line)) {
          if (parsed.type === "text") {
            onText(parsed.text);
            accumulatedOutput += parsed.text;
          } else if (parsed.type === "context_window") {
            onContextWindow(parsed.contextWindow);
          } else if (parsed.type === "result") {
            resultText = parsed.result;
            accumulatedOutput += parsed.result;
          } else if (parsed.type === "tool_call") {
            onToolCall(parsed.name, parsed.args);
          } else if (parsed.type === "session_id") {
            sessionId = parsed.sessionId;
          } else if (parsed.type === "usage") {
            usage = parsed.usage;
          } else if (parsed.type === "error") {
            streamError = parsed.message;
          }
        }
        if (!completionDetected && completionSignals.some((sig) => accumulatedOutput.includes(sig))) {
          completionDetected = true;
          interruptFiber(warningFiber);
          warningFiber = null;
        }
        resetTimer();
      },
      cwd: sandboxRepoDir,
      stdin: printCmd.stdin
    });
    if (execResult.exitCode !== 0) {
      let errorDetail = execResult.stderr;
      if (!errorDetail.trim()) {
        errorDetail = streamError ?? "";
      }
      if (!errorDetail.trim()) {
        errorDetail = resultText;
      }
      if (!errorDetail.trim()) {
        const lines = execResult.stdout.split("\n").filter((l) => l.trim());
        errorDetail = lines.slice(-20).join("\n");
      }
      return yield* new AgentError({
        message: `${provider.name} exited with code ${execResult.exitCode}:
${errorDetail}`
      });
    }
    if (streamError !== void 0) {
      return yield* new AgentError({
        message: `${provider.name} reported an error:
${streamError}`
      });
    }
    return { result: resultText || execResult.stdout, sessionId, usage };
  }).pipe(
    Effect_exports.ensuring(
      Effect_exports.sync(() => {
        interruptFiber(timeoutFiber);
        timeoutFiber = null;
        interruptFiber(warningFiber);
        warningFiber = null;
      })
    )
  );
  let raced = Effect_exports.raceFirst(execEffect, Deferred_exports.await(timeoutSignal));
  raced = Effect_exports.raceFirst(raced, Deferred_exports.await(completionTimeoutDeferred));
  if (signal) {
    raced = Effect_exports.raceFirst(raced, Deferred_exports.await(abortDeferred));
  }
  return yield* raced.pipe(
    Effect_exports.ensuring(
      Effect_exports.sync(() => {
        settled = true;
        abortCleanup?.();
        interruptFiber(timeoutFiber);
        timeoutFiber = null;
        interruptFiber(warningFiber);
        warningFiber = null;
      })
    )
  );
});
var DEFAULT_COMPLETION_SIGNAL = "<promise>COMPLETE</promise>";
var DEFAULT_IDLE_TIMEOUT_SECONDS = 10 * 60;
var DEFAULT_COMPLETION_TIMEOUT_SECONDS = 60;
var orchestrate = (options) => {
  const idleTimeoutMs = (options.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS) * 1e3;
  const completionTimeoutMs = (options.completionTimeoutSeconds ?? DEFAULT_COMPLETION_TIMEOUT_SECONDS) * 1e3;
  return Effect_exports.gen(function* () {
    const factory = yield* SandboxFactory;
    const display = yield* Display;
    const streamEmitter = yield* AgentStreamEmitter;
    const runFork = Runtime_exports.runFork(yield* Effect_exports.runtime());
    const { hostRepoDir, iterations, hooks, prompt, branch, provider } = options;
    const { sessionStorage } = provider;
    let completionSignals;
    if (options.completionSignal === void 0) {
      completionSignals = [DEFAULT_COMPLETION_SIGNAL];
    } else if (Array.isArray(options.completionSignal)) {
      completionSignals = options.completionSignal;
    } else {
      completionSignals = [options.completionSignal];
    }
    const label = (msg) => options.name !== void 0 && options.name !== "" ? `[${options.name}] ${msg}` : msg;
    const allCommits = [];
    const allIterations = [];
    let allStdout = "";
    let resolvedBranch = "";
    let iterationPreservedPath;
    const checkAbort = () => options.signal?.aborted === true ? Effect_exports.die(options.signal.reason) : Effect_exports.void;
    for (let i = 1; i <= iterations; i++) {
      yield* checkAbort();
      yield* display.status(label(`Iteration ${i}/${iterations}`), "info");
      const sandboxResult = yield* factory.withSandbox(
        ({ hostWorktreePath, sandboxRepoPath, applyToHost, bindMountHandle }, sandbox) => withSandboxLifecycle(
          {
            hostRepoDir,
            sandboxRepoDir: sandboxRepoPath,
            hooks,
            branch,
            hostWorktreePath,
            applyToHost,
            signal: options.signal,
            timeouts: options.timeouts,
            keepSourceBranch: options.keepSourceBranch
          },
          sandbox,
          (ctx) => Effect_exports.gen(function* () {
            const iterationResumeSession = i === 1 ? options.resumeSession : void 0;
            const iterationForkSession = i === 1 ? options.forkSession : void 0;
            if (iterationResumeSession !== void 0 && iterationResumeSession !== "" && bindMountHandle && sessionStorage) {
              yield* display.status(label("Resuming session"), "info");
              yield* Effect_exports.tryPromise({
                try: () => sessionStorage.resumeIntoSandbox({
                  hostCwd: hostRepoDir,
                  sandboxCwd: ctx.sandboxRepoDir,
                  sessionId: iterationResumeSession,
                  handle: bindMountHandle
                }),
                catch: (e) => new SessionCaptureError({
                  message: `Session resume failed: ${e instanceof Error ? e.message : String(e)}`,
                  sessionId: iterationResumeSession
                })
              });
            }
            const fullPrompt = options.skipPromptExpansion === true ? prompt : yield* preprocessPrompt(
              prompt,
              ctx.sandbox,
              ctx.sandboxRepoDir
            );
            yield* display.status(label("Agent started"), "success");
            const textBuffer = new TextDeltaBuffer((chunk) => {
              runFork(display.textChunk(chunk));
              runFork(
                streamEmitter.emit({
                  type: "text",
                  message: chunk,
                  iteration: i,
                  timestamp: /* @__PURE__ */ new Date()
                })
              );
            });
            const onText = (text2) => {
              textBuffer.write(text2);
            };
            const onToolCall = (name, formattedArgs) => {
              textBuffer.flush();
              runFork(display.toolCall(name, formattedArgs));
              runFork(
                streamEmitter.emit({
                  type: "toolCall",
                  name,
                  formattedArgs,
                  iteration: i,
                  timestamp: /* @__PURE__ */ new Date()
                })
              );
            };
            const onRawLine = (line) => {
              runFork(
                streamEmitter.emit({
                  type: "raw",
                  line,
                  iteration: i,
                  timestamp: /* @__PURE__ */ new Date()
                })
              );
            };
            const onIdleWarning = (minutes) => {
              const msg = minutes === 1 ? "Agent idle for 1 minute" : `Agent idle for ${minutes} minutes`;
              runFork(display.status(label(msg), "warn"));
            };
            const onCompletionTimeout = (timeoutMs) => {
              runFork(
                display.status(
                  label(
                    `Completion signal seen but agent process is hanging \u2014 force-completing after ${timeoutMs / 1e3}s grace window.`
                  ),
                  "warn"
                )
              );
            };
            let contextWindow;
            const onContextWindow = (cw) => {
              contextWindow = cw;
            };
            const {
              result: agentOutput,
              sessionId,
              usage: streamUsage
            } = yield* invokeAgent(
              ctx.sandbox,
              ctx.sandboxRepoDir,
              fullPrompt,
              provider,
              idleTimeoutMs,
              completionTimeoutMs,
              completionSignals,
              onText,
              onToolCall,
              onRawLine,
              onIdleWarning,
              onCompletionTimeout,
              onContextWindow,
              options._idleWarningIntervalMs,
              iterationResumeSession,
              iterationForkSession,
              options.signal
            );
            textBuffer.dispose();
            yield* display.status(label("Agent stopped"), "info");
            let sessionFilePath;
            let usage = streamUsage;
            if (provider.captureSessions && sessionStorage && sessionId !== void 0 && sessionId !== "" && bindMountHandle) {
              yield* display.status(label("Capturing session"), "info");
              yield* Effect_exports.tryPromise({
                try: () => sessionStorage.captureToHost({
                  hostCwd: hostRepoDir,
                  sandboxCwd: ctx.sandboxRepoDir,
                  sessionId,
                  handle: bindMountHandle
                }),
                catch: (e) => new SessionCaptureError({
                  message: `Session capture failed: ${e instanceof Error ? e.message : String(e)}`,
                  sessionId
                })
              });
              sessionFilePath = sessionStorage.hostSessionFilePath(
                hostRepoDir,
                sessionId
              );
              if (provider.parseSessionUsage || provider.parseSessionContextWindow) {
                const content = yield* Effect_exports.promise(
                  () => sessionStorage.readHostSession(hostRepoDir, sessionId).catch(() => void 0)
                );
                if (content !== void 0 && content !== "") {
                  const parsedUsage = provider.parseSessionUsage?.(content);
                  if (parsedUsage) {
                    usage = parsedUsage;
                  }
                  const parsedContextWindow = provider.parseSessionContextWindow?.(content);
                  if (parsedContextWindow) {
                    contextWindow = parsedContextWindow;
                  }
                }
              }
            }
            const matchedSignal = completionSignals.find(
              (sig) => agentOutput.includes(sig)
            );
            return {
              completionSignal: matchedSignal,
              stdout: agentOutput,
              sessionId,
              sessionFilePath,
              usage,
              contextWindow
            };
          })
        )
      ).pipe(
        // A failed iteration ends the run before run() prints the context
        // window lines, so print the completed iterations' lines here.
        Effect_exports.tapError(
          () => Effect_exports.forEach(
            buildContextWindowLines(allIterations),
            (line) => display.text(line)
          )
        )
      );
      const lifecycleResult = sandboxResult.value;
      iterationPreservedPath = sandboxResult.preservedWorktreePath;
      allCommits.push(...lifecycleResult.commits);
      allStdout += lifecycleResult.result.stdout;
      resolvedBranch = lifecycleResult.branch;
      allIterations.push({
        sessionId: lifecycleResult.result.sessionId,
        sessionFilePath: lifecycleResult.result.sessionFilePath,
        usage: lifecycleResult.result.usage,
        contextWindow: lifecycleResult.result.contextWindow
      });
      if (lifecycleResult.result.completionSignal !== void 0) {
        yield* display.status(
          label(`Agent signaled completion after ${i} iteration(s).`),
          "success"
        );
        return {
          iterations: allIterations,
          completionSignal: lifecycleResult.result.completionSignal,
          stdout: allStdout,
          commits: allCommits,
          branch: resolvedBranch,
          preservedWorktreePath: iterationPreservedPath
        };
      }
    }
    yield* display.status(
      label(`Reached max iterations (${iterations}).`),
      "info"
    );
    return {
      iterations: allIterations,
      completionSignal: void 0,
      stdout: allStdout,
      commits: allCommits,
      branch: resolvedBranch,
      preservedWorktreePath: iterationPreservedPath
    };
  });
};

// src/PromptResolver.ts
var resolvePrompt = (options) => {
  const { prompt, promptFile } = options;
  if (prompt !== void 0 && promptFile !== void 0) {
    return Effect_exports.fail(
      new PromptError({
        message: "Cannot provide both prompt and promptFile"
      })
    );
  }
  if (prompt !== void 0) {
    return Effect_exports.succeed({ text: prompt, source: "inline" });
  }
  if (promptFile === void 0) {
    return Effect_exports.fail(
      new PromptError({
        message: "Must provide either prompt or promptFile. Pass prompt: '...' or promptFile: './.sandcastle/prompt.md' to run()."
      })
    );
  }
  return Effect_exports.gen(function* () {
    const fs = yield* FileSystem_exports.FileSystem;
    const text2 = yield* fs.readFileString(promptFile).pipe(
      Effect_exports.mapError(
        (e) => new PromptError({
          message: `Failed to read prompt from ${promptFile}: ${String(e)}`
        })
      )
    );
    return { text: text2, source: "template" };
  });
};
var parseEnvFile = (filePath) => Effect_exports.gen(function* () {
  const fs = yield* FileSystem_exports.FileSystem;
  const content = yield* fs.readFileString(filePath).pipe(Effect_exports.catchAll(() => Effect_exports.succeed(null)));
  if (content === null) return {};
  const vars = {};
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) continue;
    const key = trimmed.slice(0, eqIndex).trim();
    let value = trimmed.slice(eqIndex + 1).trim();
    const isDoubleQuoted = value.length >= 2 && value.startsWith('"') && value.at(-1) === '"';
    const isSingleQuoted = value.length >= 2 && value.startsWith("'") && value.at(-1) === "'";
    if (isDoubleQuoted || isSingleQuoted) {
      value = value.slice(1, -1);
    }
    if (isDoubleQuoted) {
      value = value.replaceAll(/\\([nrt\\])/gu, (_, ch) => {
        const escapes = {
          n: "\n",
          r: "\r",
          t: "	",
          "\\": "\\"
        };
        return escapes[ch] ?? ch;
      });
    }
    vars[key] = value;
  }
  return vars;
});
var resolveEnv = (stateDir) => Effect_exports.gen(function* () {
  const sandcastleEnv = yield* parseEnvFile(join(stateDir, ".env"));
  const result = {};
  for (const [key, fileValue] of Object.entries(sandcastleEnv)) {
    const value = fileValue || (process.env[key] ?? "");
    if (value) {
      result[key] = value;
    }
  }
  return result;
});

// src/mergeProviderEnv.ts
var mergeProviderEnv = (options) => {
  const { resolvedEnv, agentProviderEnv, sandboxProviderEnv } = options;
  const agentKeys = Object.keys(agentProviderEnv);
  const sandboxKeys = new Set(Object.keys(sandboxProviderEnv));
  const overlapping = agentKeys.filter((k) => sandboxKeys.has(k));
  if (overlapping.length > 0) {
    throw new Error(
      `Overlapping env keys between agent provider and sandbox provider: ${overlapping.join(", ")}`
    );
  }
  return {
    ...resolvedEnv,
    ...sandboxProviderEnv,
    ...agentProviderEnv
  };
};

// src/PromptArgumentSubstitution.ts
var SHELL_BLOCK_PATTERN = /!`([^`]+)`/gu;
var BUILT_IN_PROMPT_ARG_KEYS = [
  "SOURCE_BRANCH",
  "TARGET_BRANCH"
];
var PLACEHOLDER_PATTERN = /\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/gu;
var placeholderKeys = (text2) => [...text2.matchAll(PLACEHOLDER_PATTERN)].map((m) => m[1] ?? "");
var validateNoArgsWithInlinePrompt = (args) => {
  if (Object.keys(args).length === 0) {
    return Effect_exports.void;
  }
  return Effect_exports.fail(
    new PromptError({
      message: 'promptArgs is only supported with promptFile. Inline prompts (prompt: "...") are passed to the agent as-is \u2014 interpolate values directly in JavaScript, or switch to promptFile to use {{KEY}} substitution.'
    })
  );
};
var validateNoBuiltInArgOverride = (args) => {
  for (const key of BUILT_IN_PROMPT_ARG_KEYS) {
    if (key in args) {
      return Effect_exports.fail(
        new PromptError({
          message: `"${key}" is a built-in prompt argument and cannot be overridden via promptArgs`
        })
      );
    }
  }
  return Effect_exports.void;
};
var findMissingPromptArgKeys = (prompt, providedArgs) => {
  const builtInSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
  const seen = /* @__PURE__ */ new Set();
  const missing = [];
  for (const key of placeholderKeys(prompt)) {
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    if (builtInSet.has(key)) {
      continue;
    }
    if (key in providedArgs && providedArgs[key] !== null && providedArgs[key] !== void 0) {
      continue;
    }
    missing.push(key);
  }
  return missing;
};
var HTML_COMMENT_PATTERN = /<!--[\s\S]*?(?:-->|$)/gu;
var markShellBlocks = (prompt) => {
  const commentRanges = [...prompt.matchAll(HTML_COMMENT_PATTERN)].map(
    (m) => [m.index, m.index + m[0].length]
  );
  return prompt.replaceAll(
    SHELL_BLOCK_PATTERN,
    (block, command, offset) => commentRanges.some(([start, end]) => offset >= start && offset < end) ? block : `!${SHELL_BLOCK_MARKER}\`${command}\``
  );
};
var substitutePromptArgs = (prompt, args, silentKeys) => {
  const markedPrompt = markShellBlocks(
    prompt.replaceAll(SHELL_BLOCK_MARKER, "")
  );
  const sanitizedArgs = Object.fromEntries(
    Object.entries(args).map(([key, value]) => [
      key,
      typeof value === "string" ? value.replaceAll(SHELL_BLOCK_MARKER, "") : value
    ])
  );
  const keys = placeholderKeys(markedPrompt);
  if (keys.length === 0 && Object.keys(sanitizedArgs).length === 0) {
    return Effect_exports.succeed(markedPrompt);
  }
  return Effect_exports.gen(function* () {
    const display = yield* Display;
    const referencedKeys = new Set(keys);
    for (const key of referencedKeys) {
      if (!(key in sanitizedArgs)) {
        return yield* new PromptError({
          message: `Prompt argument "{{${key}}}" has no matching value in promptArgs`
        });
      }
      const value = sanitizedArgs[key];
      if (value === null || value === void 0) {
        return yield* new PromptError({
          message: `Prompt argument "{{${key}}}" has value ${value === null ? "null" : "undefined"} in promptArgs`
        });
      }
    }
    for (const key of Object.keys(sanitizedArgs)) {
      if (!referencedKeys.has(key) && silentKeys?.has(key) !== true) {
        yield* display.status(
          `Prompt argument "${key}" was provided but not referenced in the prompt`,
          "warn"
        );
      }
    }
    const result = markedPrompt.replaceAll(
      PLACEHOLDER_PATTERN,
      (_match, key) => String(sanitizedArgs[key])
    );
    return result;
  });
};

// src/Output.ts
var Output = {
  /**
   * Declare an object-typed structured output extracted from an XML tag in
   * the agent's stdout. The tag contents are JSON-parsed (with fence-aware
   * unwrapping) and validated against the provided Standard Schema validator.
   *
   * Set `maxRetries` to have `run()` automatically resume the failed session
   * and ask the agent to re-emit corrected output when extraction or
   * validation fails. Default: `0` (no retries).
   */
  object: (opts) => ({
    _tag: "object",
    tag: opts.tag,
    schema: opts.schema,
    maxRetries: opts.maxRetries
  }),
  /**
   * Declare a string-typed structured output extracted from an XML tag in
   * the agent's stdout. The tag contents are whitespace-trimmed and returned
   * as a plain string — no JSON parsing, no schema validation.
   *
   * Set `maxRetries` to have `run()` automatically resume the failed session
   * and ask the agent to re-emit corrected output when extraction fails.
   * Default: `0` (no retries).
   */
  string: (opts) => ({
    _tag: "string",
    tag: opts.tag,
    maxRetries: opts.maxRetries
  })
};
var StructuredOutputError = class extends Error {
  tag;
  rawMatched;
  cause;
  commits;
  branch;
  preservedWorktreePath;
  /** Session ID of the iteration that produced the bad output, when available. */
  sessionId;
  /** Host path to the captured session JSONL, when the session was captured. */
  sessionFilePath;
  constructor(message, options) {
    super(message);
    this.name = "StructuredOutputError";
    this.tag = options.tag;
    this.rawMatched = options.rawMatched;
    this.cause = options.cause;
    this.commits = options.commits;
    this.branch = options.branch;
    this.preservedWorktreePath = options.preservedWorktreePath;
    this.sessionId = options.sessionId;
    this.sessionFilePath = options.sessionFilePath;
  }
};

// src/extractStructuredOutput.ts
var extractStructuredOutput = async (stdout, definition, context) => {
  if (definition._tag === "object") {
    return extractObject(stdout, definition, context);
  }
  return extractString(stdout, definition, context);
};
var extractObject = async (stdout, definition, context) => {
  const raw = findLastTagContent(stdout, definition.tag);
  if (raw === void 0) {
    throw new StructuredOutputError(
      `Structured output tag <${definition.tag}> not found in agent output`,
      { tag: definition.tag, rawMatched: void 0, ...context }
    );
  }
  const unwrapped = unwrapFences(raw.trim());
  let parsed;
  try {
    parsed = JSON.parse(unwrapped);
  } catch (error) {
    throw new StructuredOutputError(
      `Structured output tag <${definition.tag}> contains invalid JSON`,
      { tag: definition.tag, rawMatched: raw, cause: error, ...context }
    );
  }
  const result = await definition.schema["~standard"].validate(parsed);
  if (result.issues) {
    throw new StructuredOutputError(
      `Structured output tag <${definition.tag}> failed schema validation`,
      {
        tag: definition.tag,
        rawMatched: raw,
        cause: result.issues,
        ...context
      }
    );
  }
  return result.value;
};
var extractString = (stdout, definition, context) => {
  const raw = findLastTagContent(stdout, definition.tag);
  if (raw === void 0) {
    throw new StructuredOutputError(
      `Structured output tag <${definition.tag}> not found in agent output`,
      { tag: definition.tag, rawMatched: void 0, ...context }
    );
  }
  return raw.trim();
};
var findLastTagContent = (text2, tag) => {
  const openTag = `<${tag}>`;
  const closeTag = `</${tag}>`;
  let lastContent;
  let searchFrom = 0;
  while (true) {
    const openIdx = text2.indexOf(openTag, searchFrom);
    if (openIdx === -1) {
      break;
    }
    const contentStart = openIdx + openTag.length;
    const closeIdx = text2.indexOf(closeTag, contentStart);
    if (closeIdx === -1) {
      break;
    }
    lastContent = text2.slice(contentStart, closeIdx);
    searchFrom = closeIdx + closeTag.length;
  }
  return lastContent;
};
var unwrapFences = (text2) => {
  const fenced = /^```(?:json)?\s*\n([\s\S]*?)\n\s*```\s*$/u.exec(text2)?.[1];
  if (fenced !== void 0) {
    return fenced.trim();
  }
  return text2;
};

// src/validateTimeouts.ts
var TIMEOUTS_KEYS = [
  "copyToWorktreeMs",
  "gitSetupMs",
  "commitCollectionMs",
  "mergeToHostMs"
];
var assertPositive = (name, unit, value) => {
  if (value === void 0) {
    return;
  }
  if (typeof value !== "number" || !(value > 0)) {
    throw new Error(
      `${name} must be a positive number of ${unit}. Received: ${String(value)}`
    );
  }
};
var validateTimeoutOptions = (options) => {
  assertPositive("idleTimeoutSeconds", "seconds", options.idleTimeoutSeconds);
  assertPositive(
    "completionTimeoutSeconds",
    "seconds",
    options.completionTimeoutSeconds
  );
  for (const key of TIMEOUTS_KEYS) {
    assertPositive(`timeouts.${key}`, "milliseconds", options.timeouts?.[key]);
  }
};

// src/run.ts
var describeParserCause = (cause) => {
  if (cause === void 0) {
    return "(no parser detail)";
  }
  if (typeof cause === "string") {
    return cause;
  }
  return JSON.stringify(cause, null, 2);
};
var buildStructuredOutputRetryFeedback = (error, retriesRemaining) => {
  const raw = error.rawMatched ?? "(no matching tag was emitted)";
  const cause = describeParserCause(error.cause);
  return `Your previous response did not produce valid structured output.

Retries remaining after this attempt: ${retriesRemaining}.

Problem:
${error.message}

Parser detail:
${cause}

Previous matched output:
${raw}

Emit only a corrected <${error.tag}> block. Do not change files or run commands.`;
};
var DEFAULT_MAX_ITERATIONS = 1;
var sanitizeBranchForFilename = (branch) => branch.replaceAll(/[/\\:*?"<>|]/gu, "-");
var printFileDisplayStartup = (options) => {
  const name = options.agentName ?? "Agent";
  const label = styleText("bold", `[${name}]`);
  const branchPart = options.branch !== void 0 && options.branch !== "" ? ` on branch ${options.branch}` : "";
  const hostRepoDir = options.hostRepoDir ?? process.cwd();
  const displayLogPath = hostRepoDir === process.cwd() ? path.relative(process.cwd(), options.logPath) : options.logPath;
  console.log(`${label} Started${branchPart}`);
  console.log(styleText("dim", `  tail -f ${displayLogPath}`));
};
var buildLogFilename = (resolvedBranch, targetBranch, name) => {
  const sanitized = sanitizeBranchForFilename(resolvedBranch);
  const nameSuffix = name ? `-${name.toLowerCase().replaceAll(/[^a-z0-9_.-]/gu, (char) => "-".repeat(char.length))}` : "";
  if (targetBranch) {
    return `${sanitizeBranchForFilename(targetBranch)}-${sanitized}${nameSuffix}.log`;
  }
  return `${sanitized}${nameSuffix}.log`;
};
var buildRunSummaryRows = (options) => ({
  Agent: options.name ?? options.agentName,
  Sandbox: options.sandboxName,
  "Max iterations": String(options.maxIterations),
  Branch: options.branch
});
var buildCompletionMessage = (completionSignal, iterationsRun) => {
  if (completionSignal !== void 0) {
    return {
      message: `Run complete: agent finished after ${iterationsRun} iteration(s).`,
      severity: "success"
    };
  }
  return {
    message: `Run complete: reached ${iterationsRun} iteration(s) without completion signal.`,
    severity: "warn"
  };
};
var buildAgentStreamHandler = (logging) => {
  const userHandler = logging.type === "file" ? logging.onAgentStreamEvent : void 0;
  const verboseSink = logging.verbose === true ? buildVerboseRawLineSink(logging) : void 0;
  if (!userHandler && !verboseSink) {
    return void 0;
  }
  return (event) => {
    if (userHandler) {
      try {
        userHandler(event);
      } catch {
      }
    }
    if (verboseSink && event.type === "raw") {
      verboseSink(event.line);
    }
  };
};
var fileEndsMidLine = (filePath) => {
  let fd;
  try {
    fd = openSync(filePath, "r");
    const { size } = fstatSync(fd);
    if (size === 0) {
      return false;
    }
    const lastByte = Buffer.alloc(1);
    readSync(fd, lastByte, 0, 1, size - 1);
    return lastByte[0] !== 10;
  } catch {
    return false;
  } finally {
    if (fd !== void 0) {
      closeSync(fd);
    }
  }
};
var buildVerboseRawLineSink = (logging) => {
  if (logging.type === "file") {
    const logPath = logging.path;
    try {
      mkdirSync(path.dirname(logPath), { recursive: true });
    } catch {
    }
    return (line) => {
      try {
        const prefix = fileEndsMidLine(logPath) ? "\n" : "";
        appendFileSync(logPath, `${prefix + line}
`);
      } catch {
      }
    };
  }
  return (line) => {
    process.stdout.write(`${line}
`);
  };
};
async function run(options) {
  options.signal?.throwIfAborted();
  const {
    prompt,
    promptFile,
    maxIterations = DEFAULT_MAX_ITERATIONS,
    hooks,
    agent: provider
  } = options;
  const branchStrategy = options.branchStrategy ?? (options.sandbox.tag === "isolated" ? { type: "merge-to-head" } : { type: "head" });
  const effectiveBranchType = branchStrategy.type;
  validateTimeoutOptions(options);
  if (effectiveBranchType === "head" && options.sandbox.tag === "isolated") {
    throw new Error(
      "head branch strategy is not supported with isolated providers"
    );
  }
  if (effectiveBranchType === "head" && options.copyToWorktree && options.copyToWorktree.length > 0) {
    throw new Error(
      "copyToWorktree is not supported with head branch strategy. In head mode the host working directory is bind-mounted directly."
    );
  }
  const isResuming = options.resumeSession !== void 0 && options.resumeSession !== "";
  if (isResuming && maxIterations > 1) {
    throw new Error(
      "resumeSession cannot be combined with maxIterations > 1. Resume applies to iteration 1 only; multi-iteration resume semantics are not supported."
    );
  }
  if (options.forkSession === true && !isResuming) {
    throw new Error(
      "forkSession requires resumeSession. Use RunResult.fork(prompt) to fork the last captured session."
    );
  }
  if (options.forkSession === true) {
    assertForkSupported(provider);
  }
  if (options.output && maxIterations !== 1) {
    throw new Error(
      "output requires maxIterations to be 1. Structured output is only supported for single-iteration runs."
    );
  }
  const outputMaxRetries = options.output?.maxRetries ?? 0;
  if (outputMaxRetries < 0 || !Number.isInteger(outputMaxRetries)) {
    throw new Error(
      `output.maxRetries must be a non-negative integer. Received: ${outputMaxRetries}`
    );
  }
  if (outputMaxRetries > 0 && !provider.sessionStorage) {
    throw new Error(
      `output.maxRetries requires an agent provider that supports session resumption. The "${provider.name}" provider does not. Use an agent provider that supports resume, or set maxRetries to 0.`
    );
  }
  const branch = branchStrategy.type === "branch" ? branchStrategy.branch : void 0;
  const hostRepoDir = await Effect_exports.runPromise(
    resolveCwd(options.cwd).pipe(Effect_exports.provide(NodeContext_exports.layer))
  );
  const stateDir = resolveStateDir(
    hostRepoDir,
    options.stateDir ?? DEFAULT_STATE_DIR
  );
  if (isResuming) {
    await assertResumeSessionExists({
      provider,
      sandboxTag: options.sandbox.tag,
      hostRepoDir,
      resumeSession: options.resumeSession
    });
  }
  const resolved = await Effect_exports.runPromise(
    resolvePrompt({ prompt, promptFile }).pipe(
      Effect_exports.provide(NodeContext_exports.layer)
    )
  );
  const rawPrompt = resolved.text;
  const isInlinePrompt = resolved.source === "inline";
  if (options.output) {
    const openTag = `<${options.output.tag}>`;
    if (!rawPrompt.includes(openTag)) {
      throw new Error(
        `output tag <${options.output.tag}> not found in the resolved prompt. The caller must instruct the agent to emit the configured tag.`
      );
    }
  }
  const agentName = provider.name;
  const resolvedEnv = await Effect_exports.runPromise(
    resolveEnv(stateDir).pipe(Effect_exports.provide(NodeContext_exports.layer))
  );
  const env = mergeProviderEnv({
    resolvedEnv,
    agentProviderEnv: provider.env,
    sandboxProviderEnv: options.sandbox.env
  });
  const currentHostBranch = await Effect_exports.runPromise(
    getCurrentBranch(hostRepoDir)
  );
  const resolvedBranch = effectiveBranchType === "head" ? currentHostBranch : branch ?? generateTempBranchName(options.name);
  const targetBranch = effectiveBranchType === "merge-to-head" ? currentHostBranch : void 0;
  const resolvedLogging = options.logging ?? {
    type: "file",
    path: join(
      stateDir,
      "logs",
      buildLogFilename(resolvedBranch, targetBranch, options.name)
    )
  };
  const displayLayer = resolvedLogging.type === "file" ? (() => {
    printFileDisplayStartup({
      logPath: resolvedLogging.path,
      agentName: options.name,
      branch: resolvedBranch,
      hostRepoDir
    });
    return Layer_exports.provide(
      FileDisplay.layer(resolvedLogging.path),
      NodeFileSystem_exports.layer
    );
  })() : ClackDisplay.layer;
  const factoryLayer = Layer_exports.provide(
    WorktreeDockerSandboxFactory.layer,
    Layer_exports.mergeAll(
      Layer_exports.succeed(SandboxConfig, {
        env,
        hostRepoDir,
        stateDir,
        copyToWorktree: options.copyToWorktree,
        name: options.name,
        // Create the worktree on the temp branch already shown to the user,
        // rather than letting WorktreeManager generate a second name.
        tempBranch: effectiveBranchType === "merge-to-head" ? resolvedBranch : void 0,
        sandboxProvider: options.sandbox,
        branchStrategy,
        hooks,
        signal: options.signal,
        timeouts: options.timeouts
      }),
      NodeFileSystem_exports.layer,
      displayLayer
    )
  );
  const streamEmitterLayer = agentStreamEmitterLayer(
    buildAgentStreamHandler(resolvedLogging)
  );
  const runLayer = Layer_exports.mergeAll(
    factoryLayer,
    displayLayer,
    streamEmitterLayer
  );
  const baseEffect = Effect_exports.gen(function* () {
    const d = yield* Display;
    yield* d.intro(options.name ?? "sandcastle");
    const rows = buildRunSummaryRows({
      name: options.name,
      agentName,
      sandboxName: options.sandbox.name,
      maxIterations,
      branch: resolvedBranch
    });
    yield* d.summary("Sandcastle Run", rows);
    const userArgs = options.promptArgs ?? {};
    let resolvedPrompt;
    if (isInlinePrompt) {
      yield* validateNoArgsWithInlinePrompt(userArgs);
      resolvedPrompt = rawPrompt;
    } else {
      yield* validateNoBuiltInArgOverride(userArgs);
      const effectiveArgs = {
        SOURCE_BRANCH: resolvedBranch,
        TARGET_BRANCH: currentHostBranch,
        ...userArgs
      };
      const builtInArgKeysSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
      resolvedPrompt = yield* substitutePromptArgs(
        rawPrompt,
        effectiveArgs,
        builtInArgKeysSet
      );
    }
    const orchestrateBranch = effectiveBranchType === "head" ? currentHostBranch : branch;
    const orchestrateResult = yield* orchestrate({
      hostRepoDir,
      iterations: maxIterations,
      hooks,
      prompt: resolvedPrompt,
      branch: orchestrateBranch,
      provider,
      completionSignal: options.completionSignal,
      idleTimeoutSeconds: options.idleTimeoutSeconds,
      completionTimeoutSeconds: options.completionTimeoutSeconds,
      name: options.name,
      resumeSession: options.resumeSession,
      forkSession: options.forkSession,
      signal: options.signal,
      skipPromptExpansion: isInlinePrompt,
      timeouts: options.timeouts
    });
    const completion = buildCompletionMessage(
      orchestrateResult.completionSignal,
      orchestrateResult.iterations.length
    );
    yield* d.status(completion.message, completion.severity);
    for (const line of buildContextWindowLines(orchestrateResult.iterations)) {
      yield* d.text(line);
    }
    return orchestrateResult;
  });
  const withErrorLog = resolvedLogging.type === "file" ? baseEffect.pipe(
    Effect_exports.tapError(
      (error) => Effect_exports.gen(function* () {
        const d = yield* Display;
        yield* d.status(formatErrorMessage(error), "error");
      })
    )
  ) : baseEffect;
  let result;
  try {
    result = await Effect_exports.runPromise(
      withErrorLog.pipe(Effect_exports.provide(runLayer))
    );
  } catch (error) {
    options.signal?.throwIfAborted();
    throw error;
  }
  const baseResult = {
    ...result,
    logFilePath: resolvedLogging.type === "file" ? resolvedLogging.path : void 0,
    resume: async (followUpPrompt, resumeOptions) => {
      const lastIteration = result.iterations.at(-1);
      if (lastIteration?.sessionId === void 0 || lastIteration.sessionId === "") {
        throw new Error("Cannot resume: no sessionId was captured");
      }
      return run({
        ...options,
        // The follow-up prompt is inline: drop the template's arguments.
        promptArgs: void 0,
        // The follow-up prompt is new: structured output is opt-in per call.
        output: void 0,
        ...resumeOptions,
        prompt: followUpPrompt,
        promptFile: void 0,
        maxIterations: 1,
        resumeSession: lastIteration.sessionId
      });
    },
    fork: async (followUpPrompt, forkOptions) => {
      const lastIteration = result.iterations.at(-1);
      if (lastIteration?.sessionId === void 0 || lastIteration.sessionId === "") {
        throw new Error("Cannot fork: no sessionId was captured");
      }
      return run({
        ...options,
        // The follow-up prompt is inline: drop the template's arguments.
        promptArgs: void 0,
        // The follow-up prompt is new: structured output is opt-in per call.
        output: void 0,
        ...forkOptions,
        prompt: followUpPrompt,
        promptFile: void 0,
        maxIterations: 1,
        resumeSession: lastIteration.sessionId,
        forkSession: true
      });
    }
  };
  if (options.output) {
    const lastIteration = baseResult.iterations.at(-1);
    try {
      const output = await extractStructuredOutput(
        baseResult.stdout,
        options.output,
        {
          commits: baseResult.commits,
          branch: baseResult.branch,
          preservedWorktreePath: baseResult.preservedWorktreePath,
          sessionId: lastIteration?.sessionId,
          sessionFilePath: lastIteration?.sessionFilePath
        }
      );
      return { ...baseResult, output };
    } catch (error) {
      if (error instanceof StructuredOutputError && outputMaxRetries > 0 && error.sessionId !== void 0) {
        const retriesRemainingAfter = outputMaxRetries - 1;
        const retryOutput = {
          ...options.output,
          maxRetries: retriesRemainingAfter
        };
        return run({
          ...options,
          prompt: buildStructuredOutputRetryFeedback(
            error,
            retriesRemainingAfter
          ),
          promptFile: void 0,
          promptArgs: void 0,
          resumeSession: error.sessionId,
          forkSession: false,
          output: retryOutput
        });
      }
      throw error;
    }
  }
  return baseResult;
}

// src/raceAbortSignal.ts
var raceAbortSignal = (effect, signal) => {
  if (!signal) {
    return effect;
  }
  return Effect_exports.gen(function* () {
    if (signal.aborted) {
      return yield* Effect_exports.die(signal.reason);
    }
    const abortDeferred = yield* Deferred_exports.make();
    const runFork = Runtime_exports.runFork(yield* Effect_exports.runtime());
    const onAbort = () => {
      runFork(Deferred_exports.die(abortDeferred, signal.reason));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    return yield* Effect_exports.raceFirst(effect, Deferred_exports.await(abortDeferred)).pipe(
      Effect_exports.ensuring(
        Effect_exports.sync(() => {
          signal.removeEventListener("abort", onAbort);
        })
      )
    );
  });
};

// src/interactive.ts
var interactive = async (options) => {
  options.signal?.throwIfAborted();
  const { prompt, promptFile, hooks, agent: provider } = options;
  validateTimeoutOptions(options);
  const resolvedSandbox = options.sandbox ?? noSandbox();
  const branchStrategy = options.branchStrategy ?? (resolvedSandbox.tag === "isolated" ? { type: "merge-to-head" } : { type: "head" });
  if (branchStrategy.type === "head" && resolvedSandbox.tag === "isolated") {
    throw new Error(
      "head branch strategy is not supported with isolated providers"
    );
  }
  if (branchStrategy.type === "head" && options.copyToWorktree && options.copyToWorktree.length > 0) {
    throw new Error(
      "copyToWorktree is not supported with head branch strategy. In head mode the host working directory is bind-mounted directly."
    );
  }
  const buildInteractiveArgs = provider.buildInteractiveArgs?.bind(provider);
  if (!buildInteractiveArgs) {
    throw new Error(
      `Agent provider "${provider.name}" does not support buildInteractiveArgs, required for interactive sessions.`
    );
  }
  const branch = branchStrategy.type === "branch" ? branchStrategy.branch : void 0;
  const isHeadMode = branchStrategy.type === "head";
  const sandboxProvider = resolvedSandbox;
  const inner = Effect_exports.gen(function* () {
    const hostRepoDir = yield* resolveCwd(options.cwd);
    const stateDir = resolveStateDir(
      hostRepoDir,
      options.stateDir ?? DEFAULT_STATE_DIR
    );
    const d = yield* Display;
    const hasPromptSource = prompt !== void 0 || promptFile !== void 0;
    const resolved = hasPromptSource ? yield* resolvePrompt({ prompt, promptFile }) : void 0;
    const rawPrompt = resolved?.text ?? "";
    const isInlinePrompt = resolved?.source === "inline";
    const resolvedEnv = yield* resolveEnv(stateDir);
    const env = mergeProviderEnv({
      resolvedEnv,
      agentProviderEnv: provider.env,
      sandboxProviderEnv: sandboxProvider.env
    });
    const effectiveEnv = { ...env, ...options.env };
    const currentHostBranch = yield* getCurrentBranch(hostRepoDir);
    const resolvedBranch = branchStrategy.type === "head" ? currentHostBranch : branch ?? generateTempBranchName(options.name);
    let substitutedPrompt = rawPrompt;
    if (hasPromptSource && !isInlinePrompt) {
      const userArgs = options.promptArgs ?? {};
      yield* validateNoBuiltInArgOverride(userArgs);
      const missingKeys = findMissingPromptArgKeys(rawPrompt, userArgs);
      const collectedArgs = {};
      for (const key of missingKeys) {
        const value = yield* Effect_exports.promise(
          async () => clack.text({
            message: `Enter value for {{${key}}}`,
            validate: (v) => {
              if (v === void 0 || v === "") {
                return `A value is required for {{${key}}}`;
              }
            }
          })
        );
        if (clack.isCancel(value)) {
          clack.cancel("Prompt arg collection cancelled.");
          return yield* Effect_exports.fail(
            new Error("User cancelled prompt arg collection")
          );
        }
        collectedArgs[key] = value;
      }
      const mergedUserArgs = { ...userArgs, ...collectedArgs };
      const effectiveArgs = {
        SOURCE_BRANCH: resolvedBranch,
        TARGET_BRANCH: currentHostBranch,
        ...mergedUserArgs
      };
      const builtInArgKeysSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
      substitutedPrompt = yield* substitutePromptArgs(
        rawPrompt,
        effectiveArgs,
        builtInArgKeysSet
      );
    } else if (isInlinePrompt) {
      const userArgs = options.promptArgs ?? {};
      yield* validateNoArgsWithInlinePrompt(userArgs);
    }
    const lifecycleBranch = isHeadMode ? currentHostBranch : branch;
    yield* d.intro(options.name ?? "sandcastle interactive");
    yield* d.summary("Interactive Session", {
      Agent: options.name ?? provider.name,
      Sandbox: sandboxProvider.name,
      Branch: resolvedBranch
    });
    let worktreeInfo;
    if (!isHeadMode) {
      worktreeInfo = yield* d.taskLog(
        "Creating worktree",
        () => pruneStale(hostRepoDir, stateDir).pipe(
          Effect_exports.catchAll(() => Effect_exports.void),
          Effect_exports.andThen(
            branch !== void 0 && branch !== "" ? create(hostRepoDir, stateDir, { branch }) : create(hostRepoDir, stateDir, {
              name: options.name,
              tempBranch: resolvedBranch
            })
          )
        )
      );
    }
    const hostWorkPath = worktreeInfo?.path ?? hostRepoDir;
    const copyPaths = options.copyToWorktree;
    const onWorktreeReady = hooks?.host?.onWorktreeReady ?? [];
    const mountsToClean = [];
    const handle = yield* Effect_exports.gen(function* () {
      if (!isHeadMode) {
        if ((sandboxProvider.tag === "bind-mount" || sandboxProvider.tag === "none") && copyPaths && copyPaths.length > 0) {
          yield* d.taskLog(
            "Copying files to worktree",
            () => copyToWorktree(
              copyPaths,
              hostRepoDir,
              hostWorkPath,
              options.timeouts?.copyToWorktreeMs
            )
          );
        }
        if (onWorktreeReady.length > 0) {
          yield* runHostHooks(onWorktreeReady, hostWorkPath);
        }
      } else if (onWorktreeReady.length > 0) {
        yield* runHostHooks(onWorktreeReady, hostRepoDir);
      }
      if (sandboxProvider.tag === "none") {
        return yield* Effect_exports.promise(
          async () => sandboxProvider.create({
            worktreePath: hostWorkPath,
            env: effectiveEnv
          })
        );
      } else if (sandboxProvider.tag === "isolated") {
        const startResult2 = yield* d.taskLog(
          "Starting sandbox",
          () => startSandbox({
            provider: sandboxProvider,
            hostRepoDir: hostWorkPath,
            env: effectiveEnv,
            copyPaths: options.copyToWorktree,
            copySourceDir: hostRepoDir
          })
        );
        return startResult2.handle;
      }
      const gitPath = join(hostRepoDir, ".git");
      const rawGitMounts = yield* resolveGitMounts(gitPath);
      const gitMounts = yield* patchGitMountsForWindows(
        rawGitMounts,
        hostWorkPath,
        SANDBOX_REPO_DIR
      );
      mountsToClean.push(...gitMounts);
      const startResult = yield* d.taskLog(
        "Starting sandbox",
        () => startSandbox({
          provider: sandboxProvider,
          hostRepoDir,
          env: effectiveEnv,
          worktreeOrRepoPath: hostWorkPath,
          gitMounts,
          repoDir: SANDBOX_REPO_DIR
        })
      );
      return startResult.handle;
    }).pipe(
      Effect_exports.tapError(
        () => worktreeInfo ? remove(worktreeInfo.path).pipe(
          Effect_exports.catchAll(() => Effect_exports.void)
        ) : Effect_exports.void
      ),
      Effect_exports.onError(() => removeMountTempDirs(mountsToClean))
    );
    return yield* Effect_exports.gen(function* () {
      if (!handle.interactiveExec) {
        throw new Error(
          `Sandbox provider does not support interactiveExec. The provider must implement the optional interactiveExec method to use interactive().`
        );
      }
      const interactiveExecFn = handle.interactiveExec.bind(handle);
      const sandbox = makeSandboxFromHandle(handle);
      const { worktreePath } = handle;
      const applyToHost = sandboxProvider.tag === "isolated" && worktreeInfo ? () => syncOut(hostWorkPath, handle) : () => Effect_exports.void;
      const lifecycleEffect = withSandboxLifecycle(
        {
          hostRepoDir,
          sandboxRepoDir: worktreePath,
          hooks,
          branch: lifecycleBranch,
          hostWorktreePath: isHeadMode ? hostRepoDir : worktreeInfo?.path,
          applyToHost,
          timeouts: options.timeouts
        },
        sandbox,
        (ctx) => Effect_exports.gen(function* () {
          const fullPrompt = !hasPromptSource || isInlinePrompt ? substitutedPrompt : yield* preprocessPrompt(
            substitutedPrompt,
            ctx.sandbox,
            ctx.sandboxRepoDir
          );
          const interactiveArgs = buildInteractiveArgs({
            prompt: fullPrompt,
            dangerouslySkipPermissions: sandboxProvider.tag !== "none"
          });
          const result2 = yield* raceAbortSignal(
            Effect_exports.promise(
              async () => interactiveExecFn(interactiveArgs, {
                stdin: process.stdin,
                stdout: process.stdout,
                stderr: process.stderr,
                cwd: worktreePath
              })
            ),
            options.signal
          );
          return result2.exitCode;
        })
      );
      const lifecycleResult = yield* lifecycleEffect;
      const exitCode = lifecycleResult.result;
      let preservedWorktreePath;
      if (worktreeInfo) {
        const hasUncommitted = yield* hasUncommittedChanges(
          worktreeInfo.path
        ).pipe(Effect_exports.catchAll(() => Effect_exports.succeed(false)));
        if (hasUncommitted) {
          preservedWorktreePath = worktreeInfo.path;
        }
      }
      if (worktreeInfo && preservedWorktreePath === void 0) {
        yield* remove(worktreeInfo.path).pipe(
          Effect_exports.catchAll(() => Effect_exports.void)
        );
      }
      yield* d.summary("Session Complete", {
        Commits: String(lifecycleResult.commits.length),
        Branch: lifecycleResult.branch,
        "Exit code": String(exitCode),
        ...preservedWorktreePath === void 0 ? {} : { "Preserved worktree": preservedWorktreePath }
      });
      return {
        commits: lifecycleResult.commits,
        branch: lifecycleResult.branch,
        preservedWorktreePath,
        exitCode
      };
    }).pipe(
      // On error, always clean up worktree (on success, handled above with preserve check)
      Effect_exports.tapError(
        () => worktreeInfo ? remove(worktreeInfo.path).pipe(
          Effect_exports.catchAll(() => Effect_exports.void)
        ) : Effect_exports.void
      ),
      // Always close sandbox handle
      Effect_exports.ensuring(
        Effect_exports.promise(async () => handle.close().catch(() => {
        })).pipe(
          Effect_exports.andThen(removeMountTempDirs(mountsToClean))
        )
      )
    );
  });
  let result;
  try {
    result = await Effect_exports.runPromise(
      inner.pipe(
        Effect_exports.provide(
          Layer_exports.mergeAll(
            ClackDisplay.layer,
            NodeContext_exports.layer,
            NodeFileSystem_exports.layer
          )
        )
      )
    );
  } catch (error) {
    options.signal?.throwIfAborted();
    throw error;
  }
  return result;
};
var homeDir = () => process.env.HOME ?? "~";
var firstExistingPath = async (paths) => {
  const [head, ...rest] = paths;
  if (head === void 0) {
    return void 0;
  }
  return await pathExists(head) ? head : firstExistingPath(rest);
};
var pathExists = async (path2) => {
  try {
    await access(path2);
    return true;
  } catch {
    return false;
  }
};
var encodeProjectPath = (cwd) => {
  const isRoot = cwd === "/" || /^[A-Za-z]:[\\/]?$/u.test(cwd);
  const normalized = isRoot ? cwd : cwd.replace(/[\\/]+$/u, "");
  return normalized.replace(/^([A-Za-z]):/u, "$1").replaceAll(/[\\/]/gu, "-");
};
var claudeHostSessionPath = (cwd, id, projectsDir) => {
  const base = projectsDir ?? join(homeDir(), ".claude", "projects");
  return join(base, encodeProjectPath(cwd), `${id}.jsonl`);
};
var claudeSandboxSessionPath = (cwd, id, projectsDir) => posix.join(projectsDir, encodeProjectPath(cwd), `${id}.jsonl`);
var claudeSubagentsDirInSandbox = (cwd, id, projectsDir) => posix.join(projectsDir, encodeProjectPath(cwd), id, "subagents");
var claudeSubagentsDirOnHost = (cwd, id, projectsDir) => {
  const base = projectsDir ?? join(homeDir(), ".claude", "projects");
  return join(base, encodeProjectPath(cwd), id, "subagents");
};
var listClaudeSubagentSessionsInSandbox = async (cwd, id, handle, sandboxProjectsDir) => {
  const dir = claudeSubagentsDirInSandbox(cwd, id, sandboxProjectsDir);
  const result = await handle.exec(
    `find ${JSON.stringify(dir)} -type f -name ${JSON.stringify("agent-*.jsonl")} 2>/dev/null`
  );
  if (result.exitCode !== 0) {
    return [];
  }
  const stdout = result.stdout.trim();
  if (stdout === "") {
    return [];
  }
  return stdout.split("\n").filter((line) => line !== "");
};
var findClaudeSessionOnHost = async (id, projectsDir) => {
  const root = projectsDir ?? join(homeDir(), ".claude", "projects");
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return { path: void 0, searchedRoot: root };
  }
  const candidates = entries.filter((entry) => entry.isDirectory()).map((entry) => join(root, entry.name, `${id}.jsonl`));
  return { path: await firstExistingPath(candidates), searchedRoot: root };
};
var rewriteEntryCwd = (entry, fromCwd, toCwd) => {
  if (typeof entry.cwd === "string" && entry.cwd === fromCwd) {
    entry.cwd = toCwd;
  }
  if (entry.type === "session_meta" && isRecord(entry.payload) && typeof entry.payload.cwd === "string" && entry.payload.cwd === fromCwd) {
    entry.payload.cwd = toCwd;
  }
};
var rewriteSessionCwd = (content, fromCwd, toCwd) => {
  if (content === "") {
    return "";
  }
  return content.split("\n").map((line) => {
    if (line === "") {
      return line;
    }
    try {
      const entry = JSON.parse(line);
      if (!isRecord(entry)) {
        return line;
      }
      rewriteEntryCwd(entry, fromCwd, toCwd);
      return JSON.stringify(entry);
    } catch {
      return line;
    }
  }).join("\n");
};
var transferClaudeSession = (jsonl, fromCwd, toCwd) => rewriteSessionCwd(jsonl, fromCwd, toCwd);
var isCodexSessionFilename = (filename, id) => filename.startsWith("rollout-") && filename.endsWith(`-${id}.jsonl`);
var findCodexSessionOnHost = async (id, sessionsDir) => {
  const root = sessionsDir ?? join(homeDir(), ".codex", "sessions");
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return { path: void 0, searchedRoot: root };
  }
  for (const entry of entries) {
    const child = join(root, entry.name);
    if (entry.isFile() && isCodexSessionFilename(entry.name, id)) {
      return { path: child, searchedRoot: root };
    }
    if (entry.isDirectory()) {
      const { path: path2 } = await findCodexSessionOnHost(id, child);
      if (path2 !== void 0) {
        return { path: path2, searchedRoot: root };
      }
    }
  }
  return { path: void 0, searchedRoot: root };
};
var requireHostSessionPath = (found, id) => {
  if (found.path === void 0) {
    throw new Error(`session ${id} not found in ${found.searchedRoot}`);
  }
  return found.path;
};
var locateSandboxSessionFile = async (handle, sessionsDir, nameGlob) => {
  const result = await handle.exec(
    `find ${JSON.stringify(sessionsDir)} -type f -name ${JSON.stringify(nameGlob)} -print -quit`
  );
  const [path2 = ""] = result.stdout.trim().split("\n");
  if (result.exitCode !== 0 || path2 === "") {
    throw new Error(`session ${nameGlob} not found in ${sessionsDir}`);
  }
  return { path: path2, relativePath: posix.relative(sessionsDir, path2) };
};
var transferCodexSession = (jsonl, fromCwd, toCwd) => rewriteSessionCwd(jsonl, fromCwd, toCwd);
var encodePiSessionDir = (cwd) => {
  const stripped = cwd.replace(/^[/\\]/u, "");
  const replaced = stripped.replaceAll(/[/\\:]/gu, "-");
  return `--${replaced}--`;
};
var piSessionDirPath = (cwd, sessionsDir) => {
  const base = sessionsDir ?? join(homeDir(), ".pi", "agent", "sessions");
  return join(base, encodePiSessionDir(cwd));
};
var isPiSessionFilename = (filename, id) => filename.endsWith(`_${id}.jsonl`);
var findPiSessionOnHost = async (id, sessionsDir) => {
  const root = sessionsDir ?? join(homeDir(), ".pi", "agent", "sessions");
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return { path: void 0, searchedRoot: root };
  }
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const dirAbs = join(root, entry.name);
    let files;
    try {
      files = await readdir(dirAbs);
    } catch {
      continue;
    }
    const match = files.find((name) => isPiSessionFilename(name, id));
    if (match !== void 0) {
      return { path: join(dirAbs, match), searchedRoot: root };
    }
  }
  return { path: void 0, searchedRoot: root };
};
var transferPiSession = (jsonl, fromCwd, toCwd) => {
  if (jsonl === "") {
    return "";
  }
  return jsonl.split("\n").map((line) => {
    if (line === "") {
      return line;
    }
    try {
      const entry = JSON.parse(line);
      if (isRecord(entry) && entry.type === "session" && typeof entry.cwd === "string" && entry.cwd === fromCwd) {
        entry.cwd = toCwd;
        return JSON.stringify(entry);
      }
      return line;
    } catch {
      return line;
    }
  }).join("\n");
};
var isRecord = (value) => typeof value === "object" && value !== null;
var copilotSessionPath = (sessionsDir, id) => posix.join(sessionsDir, id, "events.jsonl");
var copilotHostSessionsDir = () => join(homeDir(), ".copilot", "session-state");
var rewriteCopilotEvent = (line, fromCwd, toCwd) => {
  let entry;
  try {
    entry = JSON.parse(line);
  } catch {
    return line;
  }
  const data = isRecord(entry) ? entry["data"] : void 0;
  const context = isRecord(data) ? data["context"] : void 0;
  if (!isRecord(context)) {
    return line;
  }
  const keys = ["cwd", "gitRoot"].filter((key) => context[key] === fromCwd);
  for (const key of keys) {
    context[key] = toCwd;
  }
  return keys.length > 0 ? JSON.stringify(entry) : line;
};
var transferCopilotSession = (jsonl, fromCwd, toCwd) => jsonl.split("\n").map(
  (line) => line === "" ? line : rewriteCopilotEvent(line, fromCwd, toCwd)
).join("\n");
var cursorChatDirName = (cwd) => createHash("md5").update(cwd).digest("hex");
var cursorHostChatsDir = () => join(homeDir(), ".cursor", "chats");
var opencodeHostSessionsDir = () => join(homeDir(), ".local", "share", "opencode", "sandcastle-sessions");
var opencodeHostDatabase = () => join(homeDir(), ".local", "share", "opencode", "opencode.db");
var sandboxCommandError = (command, result) => new Error(
  `\`${command}\` exited with code ${result.exitCode}: ${result.stderr.trim()}`
);
var captureSessionFile = (layout) => async (transfer) => {
  const { hostCwd, sandboxCwd, sessionId, handle } = transfer;
  const source = layout.sandboxPath(sandboxCwd, sessionId);
  if (layout.sandboxExport !== void 0) {
    const command = layout.sandboxExport(sessionId, source);
    const result = await handle.exec(command, { cwd: sandboxCwd });
    if (result.exitCode !== 0) {
      throw sandboxCommandError(command, result);
    }
  }
  const target = layout.hostPath(hostCwd, sessionId);
  await mkdir(dirname(target), { recursive: true });
  await handle.copyFileOut(source, target);
  if (layout.rewrite !== void 0) {
    const content = await readFile(target, "utf8");
    await writeFile(target, layout.rewrite(content, sandboxCwd, hostCwd));
  }
};
var resumeSessionFile = (layout) => async (transfer) => {
  const { hostCwd, sandboxCwd, sessionId, handle } = transfer;
  const source = layout.hostPath(hostCwd, sessionId);
  const target = layout.sandboxPath(sandboxCwd, sessionId);
  const dir = JSON.stringify(posix.dirname(target));
  if (layout.rewrite === void 0) {
    await handle.exec(`mkdir -p ${dir}`);
    await handle.copyFileIn(source, target);
    if (layout.sandboxImport !== void 0) {
      const command = layout.sandboxImport(sessionId, target);
      const result2 = await handle.exec(command, { cwd: sandboxCwd });
      if (result2.exitCode !== 0) {
        throw sandboxCommandError(command, result2);
      }
    }
    return;
  }
  const content = await readFile(source, "utf8");
  const result = await handle.exec(
    `mkdir -p ${dir} && cat > ${JSON.stringify(target)}`,
    { stdin: layout.rewrite(content, hostCwd, sandboxCwd) }
  );
  if (result.exitCode !== 0) {
    throw new Error(`writing ${target} failed: ${result.stderr}`);
  }
};
var makeSessionFileStorage = (layout) => ({
  hostSessionFilePath: (cwd, id) => layout.hostPath(cwd, id),
  existsOnHost: async (cwd, id) => pathExists(layout.hostPath(cwd, id)),
  readHostSession: async (cwd, id) => {
    const path2 = layout.hostPath(cwd, id);
    return await pathExists(path2) ? readFile(path2, "utf8") : void 0;
  },
  findByIdOnHost: async (id) => {
    let subdirs = [];
    try {
      subdirs = await readdir(layout.hostRoot);
    } catch {
    }
    return {
      path: await firstExistingPath(layout.hostPathsById(id, subdirs)),
      searchedRoot: layout.hostRoot
    };
  },
  captureToHost: captureSessionFile(layout),
  resumeIntoSandbox: resumeSessionFile(layout)
});

// src/AgentProvider.ts
var shellEscape = (s) => `'${s.replaceAll("'", String.raw`'\''`)}'`;
var contextWindowEvent = (input, ...cacheBuckets) => {
  if (typeof input !== "number") return [];
  let tokens = input;
  for (const n of cacheBuckets) if (typeof n === "number") tokens += n;
  return tokens > 0 ? [{ type: "context_window", contextWindow: { tokens } }] : [];
};
var parseClaudeContextWindow = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    if (obj.type !== "assistant" || obj.parent_tool_use_id) return [];
    const u = obj.message?.usage;
    return contextWindowEvent(
      u?.input_tokens,
      u?.cache_creation_input_tokens,
      u?.cache_read_input_tokens
    );
  } catch {
    return [];
  }
};
var TOOL_ARG_FIELDS = {
  Bash: "command",
  WebSearch: "query",
  WebFetch: "url",
  Agent: "description"
};
var messageField = (value) => typeof value === "object" && value !== null && "message" in value && typeof value.message === "string" ? value.message : void 0;
var extractErrorMessage = (obj) => {
  if (typeof obj !== "object" || obj === null) {
    return void 0;
  }
  const err = "error" in obj ? obj.error : void 0;
  if (typeof err === "string") {
    return err;
  }
  if (typeof err === "object" && err !== null) {
    const nested = messageField(err) ?? ("data" in err ? messageField(err.data) : void 0);
    if (nested !== void 0) {
      return nested;
    }
  }
  return messageField(obj);
};
var toolCallFromClaudeBlock = (block) => {
  if (block.type !== "tool_use" || typeof block.name !== "string" || block.input === void 0) {
    return void 0;
  }
  const argField = TOOL_ARG_FIELDS[block.name];
  if (argField === void 0) {
    return void 0;
  }
  const argValue = block.input[argField];
  if (typeof argValue !== "string") {
    return void 0;
  }
  return { type: "tool_call", name: block.name, args: argValue };
};
var mergeAdjacentText = (events) => {
  const merged = [];
  for (const event of events) {
    const last = merged.at(-1);
    if (event.type === "text" && last?.type === "text") {
      merged[merged.length - 1] = {
        type: "text",
        text: last.text + event.text
      };
    } else {
      merged.push(event);
    }
  }
  return merged;
};
var parseClaudeAssistantContent = (content) => mergeAdjacentText(
  content.flatMap((block) => {
    if (block.type === "text" && typeof block.text === "string") {
      return [{ type: "text", text: block.text }];
    }
    const toolCall = toolCallFromClaudeBlock(block);
    return toolCall === void 0 ? [] : [toolCall];
  })
);
var parseStreamJsonLine = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    if (obj.type === "assistant" && Array.isArray(obj.message?.content)) {
      return parseClaudeAssistantContent(
        obj.message.content
      );
    }
    if (obj.type === "result" && typeof obj.result === "string") {
      if (obj.is_error === true) {
        return [{ type: "error", message: obj.result }];
      }
      return [{ type: "result", result: obj.result }];
    }
    if (obj.type === "system" && obj.subtype === "init" && typeof obj.session_id === "string") {
      return [{ type: "session_id", sessionId: obj.session_id }];
    }
  } catch {
  }
  return [];
};
var CURSOR_PRINT_PROMPT_MAX_BYTES = 120 * 1024;
var assertCursorPrintPromptFitsArgv = (prompt) => {
  const n = Buffer.byteLength(prompt, "utf8");
  if (n > CURSOR_PRINT_PROMPT_MAX_BYTES) {
    throw new Error(
      `Cursor print-mode prompt is ${n} bytes (max ${CURSOR_PRINT_PROMPT_MAX_BYTES} bytes). The Cursor CLI accepts the prompt only as a command-line argument; shorten the prompt or split the work. Other Sandcastle providers use stdin for large prompts.`
    );
  }
};
var cursorToolCallArg = (toolCall, field) => {
  if (typeof toolCall !== "object" || toolCall === null) {
    return void 0;
  }
  const args = Reflect.get(toolCall, "args");
  if (typeof args !== "object" || args === null) {
    return void 0;
  }
  const value = Reflect.get(args, field);
  return typeof value === "string" ? value : void 0;
};
var CURSOR_TYPED_TOOL_CALLS = [
  { kind: "readToolCall", name: "Read", field: "path" },
  { kind: "writeToolCall", name: "Write", field: "path" },
  { kind: "editToolCall", name: "Edit", field: "path" },
  { kind: "shellToolCall", name: "Bash", field: "command" }
];
var parseCursorFunctionToolCall = (fn) => {
  if (typeof fn !== "object" || fn === null) {
    return [];
  }
  const name = Reflect.get(fn, "name");
  if (typeof name !== "string") {
    return [];
  }
  const rawArguments = Reflect.get(fn, "arguments");
  const rawArgs = typeof rawArguments === "string" ? rawArguments : "";
  if (rawArgs === "") {
    return [{ type: "tool_call", name, args: "" }];
  }
  try {
    const parsedArgs = JSON.parse(rawArgs);
    const command = typeof parsedArgs === "object" && parsedArgs !== null ? Reflect.get(parsedArgs, "command") : void 0;
    if (typeof command === "string") {
      return [{ type: "tool_call", name: "Bash", args: command }];
    }
  } catch {
  }
  return [{ type: "tool_call", name, args: rawArgs }];
};
var parseCursorToolCallStarted = (obj) => {
  if (obj.type !== "tool_call" || obj.subtype !== "started") {
    return [];
  }
  const toolCall = obj.tool_call;
  if (typeof toolCall !== "object" || toolCall === null) {
    return [];
  }
  for (const { kind, name, field } of CURSOR_TYPED_TOOL_CALLS) {
    const args = cursorToolCallArg(Reflect.get(toolCall, kind), field);
    if (args !== void 0) {
      return [{ type: "tool_call", name, args }];
    }
  }
  return parseCursorFunctionToolCall(Reflect.get(toolCall, "function"));
};
var parseCursorStreamLine = (line) => {
  if (!line.startsWith("{")) return [];
  let obj;
  try {
    obj = JSON.parse(line);
  } catch {
    return [];
  }
  if (obj.type === "tool_call") {
    return parseCursorToolCallStarted(obj);
  }
  return parseStreamJsonLine(line);
};
var readSandboxFile = async (handle, sandboxPath, tag) => {
  const tmpPath = join(
    tmpdir(),
    `sandcastle-${tag}-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`
  );
  await handle.copyFileOut(sandboxPath, tmpPath);
  try {
    return await readFile(tmpPath, "utf8");
  } finally {
    await rm(tmpPath, { force: true }).catch(() => {
    });
  }
};
var writeSandboxFile = async (handle, sandboxPath, content, tag) => {
  const tmpPath = join(
    tmpdir(),
    `sandcastle-${tag}-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`
  );
  await writeFile(tmpPath, content);
  try {
    await handle.exec(`mkdir -p ${JSON.stringify(posix.dirname(sandboxPath))}`);
    await handle.copyFileIn(tmpPath, sandboxPath);
  } finally {
    await rm(tmpPath, { force: true }).catch(() => {
    });
  }
};
var copyClaudeSessionFile = async ({
  handle,
  sourcePath,
  fromCwd,
  toCwd,
  destPath,
  tag
}) => {
  const jsonl = await readSandboxFile(handle, sourcePath, tag);
  const rewritten = transferClaudeSession(jsonl, fromCwd, toCwd);
  await mkdir(dirname(destPath), { recursive: true });
  await writeFile(destPath, rewritten);
};
var makeClaudeSessionStorage = (options) => {
  const hostProjectsDir = options?.sessionStorage?.hostProjectsDir;
  const sandboxProjectsDir = options?.sessionStorage?.sandboxProjectsDir ?? "/home/agent/.claude/projects";
  return {
    hostSessionFilePath: (cwd, id) => claudeHostSessionPath(cwd, id, hostProjectsDir),
    existsOnHost: (cwd, id) => pathExists(claudeHostSessionPath(cwd, id, hostProjectsDir)),
    readHostSession: async (cwd, id) => {
      const path2 = claudeHostSessionPath(cwd, id, hostProjectsDir);
      if (!await pathExists(path2)) return void 0;
      return readFile(path2, "utf8");
    },
    captureToHost: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      await copyClaudeSessionFile({
        handle,
        sourcePath: claudeSandboxSessionPath(
          sandboxCwd,
          sessionId,
          sandboxProjectsDir
        ),
        fromCwd: sandboxCwd,
        toCwd: hostCwd,
        destPath: claudeHostSessionPath(hostCwd, sessionId, hostProjectsDir),
        tag: "claude-cap"
      });
      const subagentSandboxPaths = await listClaudeSubagentSessionsInSandbox(
        sandboxCwd,
        sessionId,
        handle,
        sandboxProjectsDir
      );
      const hostSubagentsDir = claudeSubagentsDirOnHost(
        hostCwd,
        sessionId,
        hostProjectsDir
      );
      for (const sandboxSubagentPath of subagentSandboxPaths) {
        try {
          await copyClaudeSessionFile({
            handle,
            sourcePath: sandboxSubagentPath,
            fromCwd: sandboxCwd,
            toCwd: hostCwd,
            destPath: join(
              hostSubagentsDir,
              posix.basename(sandboxSubagentPath)
            ),
            tag: "claude-sub"
          });
        } catch (error) {
          console.error(
            `sandcastle: failed to capture Claude subagent transcript ${sandboxSubagentPath}: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    },
    resumeIntoSandbox: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      const hostPath = claudeHostSessionPath(
        hostCwd,
        sessionId,
        hostProjectsDir
      );
      const jsonl = await readFile(hostPath, "utf8");
      const rewritten = transferClaudeSession(jsonl, hostCwd, sandboxCwd);
      const sandboxPath = claudeSandboxSessionPath(
        sandboxCwd,
        sessionId,
        sandboxProjectsDir
      );
      await writeSandboxFile(handle, sandboxPath, rewritten, "claude-res");
    },
    findByIdOnHost: (id) => findClaudeSessionOnHost(id, hostProjectsDir)
  };
};
var makeCodexSessionStorage = (options) => {
  const hostSessionsDir = options?.sessionStorage?.hostSessionsDir;
  const sandboxSessionsDir = options?.sessionStorage?.sandboxSessionsDir ?? posix.join("/home/agent", ".codex", "sessions");
  const capturedPaths = /* @__PURE__ */ new Map();
  return {
    hostSessionFilePath: (_cwd, id) => capturedPaths.get(id),
    existsOnHost: async (_cwd, id) => {
      const found = await findCodexSessionOnHost(id, hostSessionsDir);
      return found.path !== void 0;
    },
    readHostSession: async (_cwd, id) => {
      const found = await findCodexSessionOnHost(id, hostSessionsDir);
      if (!found.path) return void 0;
      return readFile(found.path, "utf8");
    },
    captureToHost: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      const located = await locateSandboxSessionFile(
        handle,
        sandboxSessionsDir,
        `rollout-*-${sessionId}.jsonl`
      );
      const jsonl = await readSandboxFile(handle, located.path, "codex-cap");
      const rewritten = transferCodexSession(jsonl, sandboxCwd, hostCwd);
      const root = hostSessionsDir ?? join(homeDir(), ".codex", "sessions");
      const target = join(root, located.relativePath);
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, rewritten);
      capturedPaths.set(sessionId, target);
    },
    resumeIntoSandbox: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      const found = await findCodexSessionOnHost(sessionId, hostSessionsDir);
      const path2 = requireHostSessionPath(found, sessionId);
      const jsonl = await readFile(path2, "utf8");
      const rewritten = transferCodexSession(jsonl, hostCwd, sandboxCwd);
      const target = posix.join(
        sandboxSessionsDir,
        ...relative(found.searchedRoot, path2).split(/[\\/]/u)
      );
      await writeSandboxFile(handle, target, rewritten, "codex-res");
    },
    findByIdOnHost: (id) => findCodexSessionOnHost(id, hostSessionsDir)
  };
};
var piHostCaptureTarget = (hostCwd, sandboxPath, hostSessionsDir) => join(piSessionDirPath(hostCwd, hostSessionsDir), posix.basename(sandboxPath));
var makePiSessionStorage = (options) => {
  const hostSessionsDir = options?.sessionStorage?.hostSessionsDir;
  const sandboxSessionsDir = options?.sessionStorage?.sandboxSessionsDir ?? posix.join("/home/agent", ".pi", "agent", "sessions");
  const capturedPaths = /* @__PURE__ */ new Map();
  return {
    hostSessionFilePath: (cwd, id) => capturedPaths.get(id) ?? piSessionDirPath(cwd, hostSessionsDir),
    existsOnHost: async (_cwd, id) => {
      const found = await findPiSessionOnHost(id, hostSessionsDir);
      return found.path !== void 0;
    },
    readHostSession: async (_cwd, id) => {
      const { path: path2 } = await findPiSessionOnHost(id, hostSessionsDir);
      return path2 === void 0 ? void 0 : readFile(path2, "utf8");
    },
    captureToHost: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      const located = await locateSandboxSessionFile(
        handle,
        sandboxSessionsDir,
        `*_${sessionId}.jsonl`
      );
      const jsonl = await readSandboxFile(handle, located.path, "pi-cap");
      const rewritten = transferPiSession(jsonl, sandboxCwd, hostCwd);
      const target = piHostCaptureTarget(
        hostCwd,
        located.path,
        hostSessionsDir
      );
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, rewritten);
      capturedPaths.set(sessionId, target);
    },
    resumeIntoSandbox: async ({ hostCwd, sandboxCwd, sessionId, handle }) => {
      const found = await findPiSessionOnHost(sessionId, hostSessionsDir);
      const path2 = requireHostSessionPath(found, sessionId);
      const jsonl = await readFile(path2, "utf8");
      const rewritten = transferPiSession(jsonl, hostCwd, sandboxCwd);
      const filename = posix.basename(path2.replaceAll("\\", "/"));
      const target = posix.join(
        sandboxSessionsDir,
        encodePiSessionDir(sandboxCwd),
        filename
      );
      await writeSandboxFile(handle, target, rewritten, "pi-res");
    },
    findByIdOnHost: (id) => findPiSessionOnHost(id, hostSessionsDir)
  };
};
var lastAssistantMessage = (messages) => {
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg?.role === "assistant") {
      return msg;
    }
  }
  return null;
};
var parsePiAgentEnd = (messages) => {
  const msg = lastAssistantMessage(messages);
  if (msg === null) {
    return [];
  }
  if (msg.stopReason === "error") {
    return [
      {
        type: "error",
        message: typeof msg.errorMessage === "string" && msg.errorMessage ? msg.errorMessage : 'pi stopped with stopReason "error"'
      }
    ];
  }
  const texts = msg.content.flatMap(
    (block) => block.type === "text" && typeof block.text === "string" ? [block.text] : []
  );
  return texts.length > 0 ? [{ type: "result", result: texts.join("") }] : [];
};
var parsePiStreamLine = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    if (obj.type === "session" && typeof obj.id === "string") {
      return [{ type: "session_id", sessionId: obj.id }];
    }
    if (obj.type === "message_end" && obj.message?.role === "assistant") {
      const u = obj.message.usage;
      return contextWindowEvent(u?.input, u?.cacheRead, u?.cacheWrite);
    }
    if (obj.type === "message_update" && obj.assistantMessageEvent) {
      const evt = obj.assistantMessageEvent;
      if (evt.type === "text_delta" && typeof evt.delta === "string") {
        return [{ type: "text", text: evt.delta }];
      }
      return [];
    }
    if (obj.type === "tool_execution_start") {
      const rawName = obj.toolName;
      if (typeof rawName !== "string") return [];
      const toolName = Object.keys(TOOL_ARG_FIELDS).find(
        (name) => name.toLowerCase() === rawName.toLowerCase()
      );
      if (toolName === void 0) return [];
      const argField = TOOL_ARG_FIELDS[toolName];
      if (argField === void 0) return [];
      const args = obj.args;
      if (!args) return [];
      const argValue = args[argField];
      if (typeof argValue !== "string") return [];
      return [{ type: "tool_call", name: toolName, args: argValue }];
    }
    if (obj.type === "agent_error" || obj.type === "error") {
      const msg = extractErrorMessage(obj);
      return msg ? [{ type: "error", message: msg }] : [];
    }
    if (obj.type === "agent_end" && Array.isArray(obj.messages)) {
      return parsePiAgentEnd(obj.messages);
    }
  } catch {
  }
  return [];
};
var pi = (model, options) => ({
  name: "pi",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makePiSessionStorage(options),
  buildPrintCommand({
    prompt,
    resumeSession,
    forkSession
  }) {
    const thinkingFlag = options?.thinking ? ` --thinking ${options.thinking}` : "";
    const sessionFlag = resumeSession ? ` --${forkSession ? "fork" : "session"} ${shellEscape(resumeSession)}` : "";
    return {
      command: `pi -p --mode json --model ${shellEscape(model)}${thinkingFlag}${sessionFlag}`,
      stdin: prompt
    };
  },
  buildInteractiveArgs({ prompt }) {
    const args = ["pi", "--model", model];
    if (prompt) args.push(prompt);
    return args;
  },
  parseStreamLine(line) {
    return parsePiStreamLine(line);
  }
});
var parseCodexUsage = (usage) => {
  if (typeof usage !== "object" || usage === null) return void 0;
  const u = usage;
  if (typeof u.input_tokens !== "number" || typeof u.cached_input_tokens !== "number" || typeof u.output_tokens !== "number") {
    return void 0;
  }
  return {
    inputTokens: u.input_tokens - u.cached_input_tokens,
    cacheCreationInputTokens: 0,
    cacheReadInputTokens: u.cached_input_tokens,
    outputTokens: u.output_tokens
  };
};
var parseCodexSessionContextWindow = (content) => {
  const lines = content.split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i];
    if (!line.startsWith("{")) continue;
    try {
      const obj = JSON.parse(line);
      if (obj.type !== "event_msg" || obj.payload?.type !== "token_count") {
        continue;
      }
      const { info } = obj.payload;
      const tokens = info?.last_token_usage?.input_tokens;
      if (typeof tokens !== "number" || tokens <= 0) continue;
      const limit = info.model_context_window;
      return typeof limit === "number" && limit > 0 ? { tokens, limit } : { tokens };
    } catch {
    }
  }
  return void 0;
};
var parseCodexStreamLine = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    if (obj.type === "thread.started" && typeof obj.thread_id === "string") {
      return [{ type: "session_id", sessionId: obj.thread_id }];
    }
    if (obj.type === "item.completed" && obj.item?.type === "agent_message" && typeof obj.item.text === "string") {
      const { text: text2 } = obj.item;
      return [
        { type: "text", text: text2 },
        { type: "result", result: text2 }
      ];
    }
    if (obj.type === "item.started" && obj.item?.type === "command_execution" && typeof obj.item.command === "string") {
      return [{ type: "tool_call", name: "Bash", args: obj.item.command }];
    }
    if (obj.type === "error") {
      const msg = extractErrorMessage(obj);
      return msg ? [{ type: "result", result: msg }] : [];
    }
    if (obj.type === "turn.failed") {
      const msg = extractErrorMessage(obj);
      return [{ type: "error", message: msg ?? "codex turn failed" }];
    }
    if (obj.type === "turn.completed") {
      const usage = parseCodexUsage(obj.usage);
      return usage ? [{ type: "usage", usage }] : [];
    }
  } catch {
  }
  return [];
};
var codex = (model, options) => ({
  name: "codex",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makeCodexSessionStorage(options),
  buildPrintCommand({
    prompt,
    resumeSession,
    forkSession
  }) {
    const effortFlag = options?.effort ? ` -c ${shellEscape(`model_reasoning_effort="${options.effort}"`)}` : "";
    const approvalsFlags = options?.approvalsReviewer === "auto_review" ? ` -a on-request -s danger-full-access -c ${shellEscape(`approvals_reviewer="auto_review"`)}` : " --dangerously-bypass-approvals-and-sandbox";
    let base;
    if (resumeSession && forkSession) {
      base = `codex exec fork ${shellEscape(resumeSession)}`;
    } else if (resumeSession) {
      base = `codex exec resume ${shellEscape(resumeSession)}`;
    } else {
      base = "codex exec";
    }
    const stdinArg = resumeSession ? " -" : "";
    return {
      command: `${base} --json${approvalsFlags} -m ${shellEscape(model)}${effortFlag}${stdinArg}`,
      stdin: prompt
    };
  },
  buildInteractiveArgs({ prompt }) {
    const args = ["codex", "--model", model];
    if (prompt) args.push(prompt);
    return args;
  },
  parseStreamLine(line) {
    return parseCodexStreamLine(line);
  },
  parseSessionContextWindow(content) {
    return parseCodexSessionContextWindow(content);
  }
});
var makeCursorSessionStorage = (options) => {
  const hostDir = options?.sessionStorage?.hostChatsDir ?? cursorHostChatsDir();
  const sandboxDir = options?.sessionStorage?.sandboxChatsDir ?? "/home/agent/.cursor/chats";
  return {
    ...makeSessionFileStorage({
      hostRoot: hostDir,
      hostPath: (cwd, id) => join(hostDir, cursorChatDirName(cwd), id, "store.db"),
      hostPathsById: (id, chatDirs) => chatDirs.map((chatDir) => join(hostDir, chatDir, id, "store.db")),
      sandboxPath: (cwd, id) => posix.join(sandboxDir, cursorChatDirName(cwd), id, "store.db")
    }),
    canFork: false
  };
};
var cursor = (model, options) => ({
  name: "cursor",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makeCursorSessionStorage(options),
  buildPrintCommand({
    prompt,
    dangerouslySkipPermissions,
    resumeSession,
    forkSession
  }) {
    if (resumeSession !== void 0 && forkSession === true) {
      throw new ForkNotSupportedError({
        message: "cursor cannot fork a session: its CLI resumes but has no fork. Use resume() to continue the session in place.",
        agentProvider: "cursor"
      });
    }
    assertCursorPrintPromptFitsArgv(prompt);
    const forceFlag = dangerouslySkipPermissions ? " --force" : "";
    const resumeFlag = resumeSession === void 0 ? "" : ` --resume=${shellEscape(resumeSession)}`;
    return {
      command: `agent --print --output-format stream-json --model ${shellEscape(model)} ${forceFlag}${resumeFlag} ${shellEscape(prompt)}`
    };
  },
  buildInteractiveArgs({
    prompt,
    dangerouslySkipPermissions
  }) {
    const args = ["agent", "--model", model];
    if (dangerouslySkipPermissions) args.push("--force");
    if (prompt) args.push(prompt);
    return args;
  },
  parseStreamLine(line) {
    return parseCursorStreamLine(line);
  }
});
var OPENCODE_TOOL_ARG_FIELDS = {
  bash: "command",
  webfetch: "url",
  task: "description"
};
var parseOpenCodeStreamLine = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    const { part } = obj;
    if (obj.type === "step_start" && typeof obj.sessionID === "string") {
      return [{ type: "session_id", sessionId: obj.sessionID }];
    }
    if (obj.type === "step_finish" && part?.tokens) {
      const t = part.tokens;
      return contextWindowEvent(t.input, t.cache?.read, t.cache?.write);
    }
    if (obj.type === "text" && part?.type === "text" && typeof part.text === "string") {
      return [
        { type: "text", text: part.text },
        { type: "result", result: part.text }
      ];
    }
    if (obj.type === "tool_use" && part?.type === "tool") {
      if (typeof part.tool !== "string") return [];
      const state = part.state;
      if (state?.status !== "completed") return [];
      const { input } = state;
      if (!input) return [];
      const argField = OPENCODE_TOOL_ARG_FIELDS[part.tool];
      const argValue = argField === void 0 ? void 0 : input[argField];
      const args = typeof argValue === "string" ? argValue : JSON.stringify(input);
      return [{ type: "tool_call", name: part.tool, args }];
    }
    if (obj.type === "error") {
      const msg = extractErrorMessage(obj);
      return msg ? [{ type: "error", message: msg }] : [];
    }
  } catch {
  }
  return [];
};
var makeOpenCodeSessionStorage = (options) => {
  const hostDir = options?.sessionStorage?.hostSessionsDir ?? opencodeHostSessionsDir();
  const exportDir = options?.sessionStorage?.sandboxExportDir ?? "/tmp";
  const hostPath = (id) => join(hostDir, `${id}.json`);
  return makeSessionFileStorage({
    hostRoot: hostDir,
    hostPath: (_cwd, id) => hostPath(id),
    // A no-sandbox run resumes from OpenCode's own database, which can't be
    // queried by id from here; OpenCode reports an unknown id itself.
    hostPathsById: (id) => [hostPath(id), opencodeHostDatabase()],
    sandboxPath: (_cwd, id) => posix.join(exportDir, `${id}.json`),
    sandboxExport: (id, path2) => `opencode export ${shellEscape(id)} > ${shellEscape(path2)}`,
    sandboxImport: (_id, path2) => `opencode import ${shellEscape(path2)}`
  });
};
var opencode = (model, options) => ({
  name: "opencode",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makeOpenCodeSessionStorage(options),
  buildPrintCommand({
    prompt,
    dangerouslySkipPermissions,
    resumeSession,
    forkSession
  }) {
    const variantFlag = options?.variant ? ` --variant ${shellEscape(options.variant)}` : "";
    const agentFlag = options?.agent ? ` --agent ${shellEscape(options.agent)}` : "";
    const permissionsFlag = dangerouslySkipPermissions ? " --dangerously-skip-permissions" : "";
    const sessionFlags = resumeSession === void 0 ? "" : ` --session ${shellEscape(resumeSession)}${forkSession === true ? " --fork" : ""}`;
    return {
      command: `opencode run --format json --model ${shellEscape(model)}${variantFlag}${agentFlag}${permissionsFlag}${sessionFlags} ${shellEscape(prompt)}`
    };
  },
  buildInteractiveArgs({ prompt }) {
    const args = ["opencode", "--model", model];
    if (options?.agent) args.push("--agent", options.agent);
    if (prompt) args.push("--prompt", prompt);
    return args;
  },
  parseStreamLine(line) {
    return parseOpenCodeStreamLine(line);
  }
});
var COPILOT_PRINT_PROMPT_MAX_BYTES = 120 * 1024;
var assertCopilotPrintPromptFitsArgv = (prompt) => {
  const n = Buffer.byteLength(prompt, "utf8");
  if (n > COPILOT_PRINT_PROMPT_MAX_BYTES) {
    throw new Error(
      `Copilot print-mode prompt is ${n} bytes (max ${COPILOT_PRINT_PROMPT_MAX_BYTES} bytes). This provider passes the prompt as a command-line argument; shorten the prompt or split the work. Other Sandcastle providers use stdin for large prompts.`
    );
  }
};
var parseCopilotStreamLine = (line) => {
  if (!line.startsWith("{")) return [];
  try {
    const obj = JSON.parse(line);
    if (obj.type === "assistant.message_delta" && typeof obj.data?.deltaContent === "string") {
      return [{ type: "text", text: obj.data.deltaContent }];
    }
    if (obj.type === "tool.execution_start") {
      const rawName = obj.data?.toolName;
      if (typeof rawName !== "string") return [];
      const toolName = rawName === "bash" ? "Bash" : rawName;
      const argField = TOOL_ARG_FIELDS[toolName];
      if (argField === void 0) return [];
      const args = obj.data?.arguments;
      if (!args) return [];
      const argValue = args[argField];
      if (typeof argValue !== "string") return [];
      return [{ type: "tool_call", name: toolName, args: argValue }];
    }
    if (obj.type === "assistant.message" && typeof obj.data?.content === "string" && obj.data.content.length > 0) {
      return [{ type: "result", result: obj.data.content }];
    }
    if (obj.type === "session.usage_checkpoint") {
      return contextWindowEvent(
        obj.data?.accountingSnapshot?.lastCallInputTokens
      );
    }
    if (obj.type === "result" && typeof obj.sessionId === "string") {
      return [{ type: "session_id", sessionId: obj.sessionId }];
    }
    if (obj.type === "error" || obj.type === "agent_error") {
      const msg = extractErrorMessage(obj);
      return msg ? [{ type: "error", message: msg }] : [];
    }
  } catch {
  }
  return [];
};
var makeCopilotSessionStorage = (options) => {
  const hostDir = options?.sessionStorage?.hostSessionsDir ?? copilotHostSessionsDir();
  const sandboxDir = options?.sessionStorage?.sandboxSessionsDir ?? "/home/agent/.copilot/session-state";
  const hostPath = (id) => join(hostDir, id, "events.jsonl");
  return {
    ...makeSessionFileStorage({
      hostRoot: hostDir,
      hostPath: (_cwd, id) => hostPath(id),
      hostPathsById: (id) => [hostPath(id)],
      sandboxPath: (_cwd, id) => copilotSessionPath(sandboxDir, id),
      rewrite: transferCopilotSession
    }),
    canFork: false
  };
};
var copilot = (model, options) => ({
  name: "copilot",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makeCopilotSessionStorage(options),
  buildPrintCommand({
    prompt,
    dangerouslySkipPermissions,
    resumeSession,
    forkSession
  }) {
    if (resumeSession !== void 0 && forkSession === true) {
      throw new ForkNotSupportedError({
        message: "copilot cannot fork a session: its CLI resumes but has no fork. Use resume() to continue the session in place.",
        agentProvider: "copilot"
      });
    }
    assertCopilotPrintPromptFitsArgv(prompt);
    const allowAll = dangerouslySkipPermissions ? " --allow-all-tools" : "";
    const effortFlag = options?.effort ? ` --effort ${options.effort}` : "";
    const resumeFlag = resumeSession === void 0 ? "" : ` --resume=${shellEscape(resumeSession)}`;
    return {
      command: `copilot -p ${shellEscape(prompt)} --output-format json --model ${shellEscape(model)}${allowAll}${effortFlag}${resumeFlag}`
    };
  },
  buildInteractiveArgs({ prompt }) {
    const args = ["copilot", "--model", model];
    if (prompt) args.push("-i", prompt);
    return args;
  },
  parseStreamLine(line) {
    return parseCopilotStreamLine(line);
  }
});
var claudeCode = (model, options) => ({
  name: "claude-code",
  env: options?.env ?? {},
  captureSessions: options?.captureSessions ?? true,
  sessionStorage: makeClaudeSessionStorage(options),
  buildPrintCommand({
    prompt,
    dangerouslySkipPermissions,
    resumeSession,
    forkSession
  }) {
    const skipPermissionsFlag = dangerouslySkipPermissions ? " --dangerously-skip-permissions" : "";
    const permissionFlag = options?.permissionMode ? ` --permission-mode ${options.permissionMode}` : skipPermissionsFlag;
    const effortFlag = options?.effort ? ` --effort ${options.effort}` : "";
    const resumeFlag = resumeSession ? ` --resume ${shellEscape(resumeSession)}` : "";
    const forkFlag = resumeSession && forkSession ? " --fork-session" : "";
    return {
      command: `claude --print --verbose${permissionFlag} --output-format stream-json --model ${shellEscape(model)}${effortFlag}${resumeFlag}${forkFlag} -p -`,
      stdin: prompt
    };
  },
  buildInteractiveArgs({
    prompt,
    dangerouslySkipPermissions
  }) {
    const args = ["claude"];
    if (options?.permissionMode) {
      args.push("--permission-mode", options.permissionMode);
    } else if (dangerouslySkipPermissions) {
      args.push("--dangerously-skip-permissions");
    }
    args.push("--model", model);
    if (options?.effort) args.push("--effort", options.effort);
    if (prompt) args.push(prompt);
    return args;
  },
  parseStreamLine(line) {
    return [...parseStreamJsonLine(line), ...parseClaudeContextWindow(line)];
  },
  parseSessionUsage(content) {
    const lines = content.split("\n");
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i];
      if (!line.startsWith("{")) continue;
      try {
        const obj = JSON.parse(line);
        if (obj.type === "assistant" && obj.message?.usage) {
          const u = obj.message.usage;
          if (typeof u.input_tokens === "number" && typeof u.cache_creation_input_tokens === "number" && typeof u.cache_read_input_tokens === "number" && typeof u.output_tokens === "number") {
            return {
              inputTokens: u.input_tokens,
              cacheCreationInputTokens: u.cache_creation_input_tokens,
              cacheReadInputTokens: u.cache_read_input_tokens,
              outputTokens: u.output_tokens
            };
          }
        }
      } catch {
      }
    }
    return void 0;
  }
});

// src/createSandbox.ts
var withAgentEnv = (agent, sandboxProviderEnv) => {
  const entries = Object.entries(agent.env);
  if (entries.length === 0) {
    return agent;
  }
  mergeProviderEnv({
    resolvedEnv: {},
    agentProviderEnv: agent.env,
    sandboxProviderEnv
  });
  const invalid = entries.map(([key]) => key).filter((key) => !/^[A-Za-z_][A-Za-z0-9_]*$/u.test(key));
  if (invalid.length > 0) {
    throw new Error(
      `Invalid agent provider env key(s): ${invalid.join(", ")}. Keys must match [A-Za-z_][A-Za-z0-9_]*.`
    );
  }
  const exports$1 = entries.map(([key, value]) => `export ${key}=${shellEscape(value)}; `).join("");
  return {
    ...agent,
    buildPrintCommand: (options) => {
      const printCommand = agent.buildPrintCommand(options);
      return { ...printCommand, command: exports$1 + printCommand.command };
    }
  };
};
var buildSandboxHandle = (ctx, close) => {
  const {
    branch,
    worktreePath,
    hostRepoDir,
    stateDir,
    sandboxRepoDir,
    sandbox,
    providerHandle,
    bindMountHandle,
    applyToHost,
    timeouts,
    branchStrategy
  } = ctx;
  const mergeToHead = branchStrategy?.type === "merge-to-head";
  const sandboxHandle = {
    branch,
    worktreePath,
    run: async (runOptions) => {
      runOptions.signal?.throwIfAborted();
      const { prompt, promptFile, maxIterations = 1 } = runOptions;
      const provider = withAgentEnv(runOptions.agent, ctx.sandboxProviderEnv);
      validateTimeoutOptions(runOptions);
      const isResuming = runOptions.resumeSession !== void 0 && runOptions.resumeSession !== "";
      if (isResuming && maxIterations > 1) {
        throw new Error(
          "resumeSession cannot be combined with maxIterations > 1. Resume applies to iteration 1 only; multi-iteration resume semantics are not supported."
        );
      }
      if (runOptions.forkSession === true && !isResuming) {
        throw new Error(
          "forkSession requires resumeSession. Use sandboxRunResult.fork(prompt) to fork the most recent captured session."
        );
      }
      if (runOptions.forkSession === true) {
        assertForkSupported(provider);
      }
      if (isResuming) {
        await assertResumeSessionExists({
          provider,
          sandboxTag: ctx.providerTag,
          hostRepoDir,
          resumeSession: runOptions.resumeSession
        });
      }
      const resolved = await Effect_exports.runPromise(
        resolvePrompt({ prompt, promptFile }).pipe(
          Effect_exports.provide(NodeContext_exports.layer)
        )
      );
      const rawPrompt = resolved.text;
      const isInlinePrompt = resolved.source === "inline";
      const userArgs = runOptions.promptArgs ?? {};
      const currentHostBranch = await Effect_exports.runPromise(
        getCurrentBranch(hostRepoDir)
      );
      const displayRef = Ref_exports.unsafeMake([]);
      const silentDisplayLayer = SilentDisplay.layer(displayRef);
      const resolvedPrompt = await Effect_exports.runPromise(
        Effect_exports.gen(function* () {
          if (isInlinePrompt) {
            yield* validateNoArgsWithInlinePrompt(userArgs);
            return rawPrompt;
          }
          yield* validateNoBuiltInArgOverride(userArgs);
          const effectiveArgs = {
            SOURCE_BRANCH: branch,
            TARGET_BRANCH: currentHostBranch,
            ...userArgs
          };
          const builtInArgKeysSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
          return yield* substitutePromptArgs(
            rawPrompt,
            effectiveArgs,
            builtInArgKeysSet
          );
        }).pipe(Effect_exports.provide(silentDisplayLayer))
      );
      const resolvedLogging = runOptions.logging ?? {
        type: "file",
        path: join(
          stateDir,
          "logs",
          buildLogFilename(branch, void 0, runOptions.name)
        )
      };
      const runDisplayLayer = resolvedLogging.type === "file" ? (() => {
        printFileDisplayStartup({
          logPath: resolvedLogging.path,
          agentName: runOptions.name,
          branch
        });
        return Layer_exports.provide(
          FileDisplay.layer(resolvedLogging.path),
          NodeFileSystem_exports.layer
        );
      })() : silentDisplayLayer;
      const reuseFactoryLayer = reuseSandboxFactoryLayer(
        {
          hostWorktreePath: worktreePath,
          sandboxRepoPath: sandboxRepoDir,
          applyToHost,
          bindMountHandle
        },
        sandbox
      );
      const streamEmitterLayer = agentStreamEmitterLayer(
        buildAgentStreamHandler(resolvedLogging)
      );
      const runLayer = Layer_exports.mergeAll(
        reuseFactoryLayer,
        runDisplayLayer,
        streamEmitterLayer
      );
      let result;
      try {
        result = await Effect_exports.runPromise(
          Effect_exports.gen(function* () {
            const display = yield* Display;
            yield* display.intro(runOptions.name ?? "sandcastle");
            const orchestrateResult = yield* orchestrate({
              hostRepoDir,
              iterations: maxIterations,
              prompt: resolvedPrompt,
              branch: mergeToHead ? void 0 : branch,
              provider,
              completionSignal: runOptions.completionSignal,
              idleTimeoutSeconds: runOptions.idleTimeoutSeconds,
              completionTimeoutSeconds: runOptions.completionTimeoutSeconds,
              name: runOptions.name,
              resumeSession: runOptions.resumeSession,
              forkSession: runOptions.forkSession,
              signal: runOptions.signal,
              skipPromptExpansion: isInlinePrompt,
              timeouts,
              keepSourceBranch: mergeToHead
            });
            const completion = buildCompletionMessage(
              orchestrateResult.completionSignal,
              orchestrateResult.iterations.length
            );
            yield* display.status(completion.message, completion.severity);
            for (const line of buildContextWindowLines(
              orchestrateResult.iterations
            )) {
              yield* display.text(line);
            }
            return orchestrateResult;
          }).pipe(Effect_exports.provide(runLayer))
        );
      } catch (error) {
        runOptions.signal?.throwIfAborted();
        throw error;
      }
      const baseResult = {
        iterations: result.iterations,
        completionSignal: result.completionSignal,
        stdout: result.stdout,
        commits: result.commits,
        logFilePath: resolvedLogging.type === "file" ? resolvedLogging.path : void 0
      };
      const lastIteration = result.iterations.at(-1);
      if (provider.sessionStorage && lastIteration?.sessionId !== void 0 && lastIteration.sessionId !== "") {
        const capturedSessionId = lastIteration.sessionId;
        return {
          ...baseResult,
          resume: (nextPrompt, resumeOptions) => sandboxHandle.run({
            ...runOptions,
            // The follow-up prompt is inline: drop the template's arguments.
            promptArgs: void 0,
            ...resumeOptions,
            prompt: nextPrompt,
            promptFile: void 0,
            maxIterations: 1,
            resumeSession: capturedSessionId
          }),
          fork: (nextPrompt, forkOptions) => sandboxHandle.run({
            ...runOptions,
            // The follow-up prompt is inline: drop the template's arguments.
            promptArgs: void 0,
            ...forkOptions,
            prompt: nextPrompt,
            promptFile: void 0,
            maxIterations: 1,
            resumeSession: capturedSessionId,
            forkSession: true
          })
        };
      }
      return baseResult;
    },
    interactive: async (interactiveOptions) => {
      interactiveOptions.signal?.throwIfAborted();
      const { agent: provider, prompt, promptFile } = interactiveOptions;
      const buildInteractiveArgs = provider.buildInteractiveArgs?.bind(provider);
      if (!buildInteractiveArgs) {
        throw new Error(
          `Agent provider "${provider.name}" does not support buildInteractiveArgs, required for interactive sessions.`
        );
      }
      if (!providerHandle?.interactiveExec) {
        throw new Error(
          `Sandbox provider does not support interactiveExec. The provider must implement the optional interactiveExec method to use interactive().`
        );
      }
      const interactiveExecFn = providerHandle.interactiveExec.bind(providerHandle);
      let lifecycleResult;
      try {
        lifecycleResult = await Effect_exports.runPromise(
          Effect_exports.gen(function* () {
            const resolved = yield* resolvePrompt({ prompt, promptFile });
            const rawPrompt = resolved.text;
            const isInlinePrompt = resolved.source === "inline";
            const userArgs = interactiveOptions.promptArgs ?? {};
            const currentHostBranch = yield* getCurrentBranch(hostRepoDir);
            let resolvedPrompt;
            if (isInlinePrompt) {
              yield* validateNoArgsWithInlinePrompt(userArgs);
              resolvedPrompt = rawPrompt;
            } else {
              yield* validateNoBuiltInArgOverride(userArgs);
              const effectiveArgs = {
                SOURCE_BRANCH: branch,
                TARGET_BRANCH: currentHostBranch,
                ...userArgs
              };
              const builtInArgKeysSet = new Set(
                BUILT_IN_PROMPT_ARG_KEYS
              );
              resolvedPrompt = yield* substitutePromptArgs(
                rawPrompt,
                effectiveArgs,
                builtInArgKeysSet
              );
            }
            return yield* withSandboxLifecycle(
              {
                hostRepoDir,
                sandboxRepoDir,
                branch: mergeToHead ? void 0 : branch,
                hostWorktreePath: worktreePath,
                applyToHost,
                timeouts,
                keepSourceBranch: mergeToHead
              },
              sandbox,
              (ctx2) => Effect_exports.gen(function* () {
                const fullPrompt = isInlinePrompt ? resolvedPrompt : yield* preprocessPrompt(
                  resolvedPrompt,
                  ctx2.sandbox,
                  ctx2.sandboxRepoDir
                );
                const interactiveArgs = buildInteractiveArgs({
                  prompt: fullPrompt,
                  dangerouslySkipPermissions: true
                });
                const execPromise = interactiveExecFn(interactiveArgs, {
                  stdin: process.stdin,
                  stdout: process.stdout,
                  stderr: process.stderr,
                  cwd: sandboxRepoDir
                });
                const result = yield* raceAbortSignal(
                  Effect_exports.promise(() => execPromise),
                  interactiveOptions.signal
                );
                return result.exitCode;
              })
            );
          }).pipe(
            Effect_exports.provide(
              Layer_exports.mergeAll(ClackDisplay.layer, NodeContext_exports.layer)
            )
          )
        );
      } catch (error) {
        interactiveOptions.signal?.throwIfAborted();
        throw error;
      }
      return {
        commits: lifecycleResult.commits,
        exitCode: lifecycleResult.result
      };
    },
    exec: async (command, options) => {
      const mergedOptions = { cwd: sandboxRepoDir, ...options };
      if (providerHandle) {
        return providerHandle.exec(command, mergedOptions);
      }
      return Effect_exports.runPromise(sandbox.exec(command, mergedOptions));
    },
    close: async () => close(),
    [Symbol.asyncDispose]: async () => {
      await sandboxHandle.close();
    }
  };
  return sandboxHandle;
};
var createSandboxFromWorktree = async (options) => {
  const { branch, worktreePath, hostRepoDir } = options;
  const isTestMode = !!options._test?.buildSandbox;
  validateTimeoutOptions(options);
  if (options.copyToWorktree && options.copyToWorktree.length > 0 && options.sandbox.tag !== "isolated") {
    await Effect_exports.runPromise(
      copyToWorktree(
        options.copyToWorktree,
        hostRepoDir,
        worktreePath,
        options.timeouts?.copyToWorktreeMs
      )
    );
  }
  let providerHandle;
  let sandbox;
  let sandboxRepoDir;
  const isIsolated = options.sandbox.tag === "isolated";
  const mountsToClean = [];
  if (isTestMode) {
    sandbox = options._test.buildSandbox(worktreePath);
    sandboxRepoDir = worktreePath;
    providerHandle = options._test.bindMountHandle;
  } else {
    const resolvedEnv = await Effect_exports.runPromise(
      resolveEnv(options.stateDir).pipe(Effect_exports.provide(NodeContext_exports.layer))
    );
    const env = mergeProviderEnv({
      resolvedEnv,
      agentProviderEnv: {},
      sandboxProviderEnv: options.sandbox.env
    });
    const provider = options.sandbox;
    let startEffect;
    if (provider.tag === "isolated") {
      startEffect = startSandbox({
        provider,
        hostRepoDir: worktreePath,
        env,
        copyPaths: options.copyToWorktree,
        copySourceDir: hostRepoDir
      });
    } else if (provider.tag === "none") {
      startEffect = startSandbox({
        provider,
        hostRepoDir,
        env,
        worktreeOrRepoPath: worktreePath
      });
    } else {
      startEffect = resolveGitMounts(join(hostRepoDir, ".git")).pipe(
        Effect_exports.provide(NodeFileSystem_exports.layer),
        Effect_exports.catchAll(() => Effect_exports.succeed([])),
        // Patch git mounts for Windows worktree compatibility (ADR-0006)
        Effect_exports.flatMap(
          (gitMounts) => patchGitMountsForWindows(gitMounts, worktreePath, SANDBOX_REPO_DIR)
        ),
        Effect_exports.tap((gitMounts) => {
          mountsToClean.push(...gitMounts);
        }),
        Effect_exports.flatMap(
          (gitMounts) => startSandbox({
            provider,
            hostRepoDir,
            env,
            worktreeOrRepoPath: worktreePath,
            gitMounts,
            repoDir: SANDBOX_REPO_DIR
          })
        ),
        Effect_exports.onError(() => removeMountTempDirs(mountsToClean))
      );
    }
    const startResult = await Effect_exports.runPromise(startEffect);
    providerHandle = startResult.handle;
    sandbox = startResult.sandbox;
    sandboxRepoDir = startResult.worktreePath;
  }
  const sandboxOnReady = options.hooks?.sandbox?.onSandboxReady;
  const hostOnReady = options.hooks?.host?.onSandboxReady;
  if ((sandboxOnReady?.length ?? 0) > 0 || (hostOnReady?.length ?? 0) > 0) {
    await Effect_exports.runPromise(
      Effect_exports.gen(function* () {
        yield* sandbox.exec(
          `git config --global --add safe.directory "${sandboxRepoDir}"`
        );
        const sandboxEffects = (sandboxOnReady ?? []).map(
          (hook) => sandbox.exec(hook.command, {
            cwd: sandboxRepoDir,
            sudo: hook.sudo
          })
        );
        const allEffects = [
          ...sandboxEffects,
          ...hostOnReady !== void 0 && hostOnReady.length > 0 ? [runHostHooks(hostOnReady, worktreePath)] : []
        ];
        yield* Effect_exports.all(allEffects, {
          concurrency: "unbounded"
        });
      })
    );
  }
  const applyToHost = isIsolated && providerHandle ? () => syncOut(worktreePath, providerHandle) : () => Effect_exports.void;
  let closed = false;
  const bindMountHandle = options.sandbox.tag === "bind-mount" ? providerHandle : void 0;
  return buildSandboxHandle(
    {
      branch,
      worktreePath,
      hostRepoDir,
      stateDir: options.stateDir,
      sandboxRepoDir,
      sandbox,
      providerHandle,
      bindMountHandle,
      providerTag: options.sandbox.tag,
      sandboxProviderEnv: options.sandbox.env,
      applyToHost,
      timeouts: options.timeouts,
      branchStrategy: options.branchStrategy
    },
    async () => {
      if (closed) {
        return { preservedWorktreePath: void 0 };
      }
      closed = true;
      if (providerHandle) {
        await providerHandle.close();
      }
      await Effect_exports.runPromise(removeMountTempDirs(mountsToClean));
      return { preservedWorktreePath: void 0 };
    }
  );
};
var createSandbox = async (options) => {
  const { branch } = options;
  const isTestMode = !!options._test?.buildSandbox;
  const isIsolated = options.sandbox.tag === "isolated";
  validateTimeoutOptions(options);
  const mountsToClean = [];
  const {
    hostRepoDir,
    stateDir,
    worktreePath,
    providerHandle,
    sandbox,
    sandboxRepoDir
  } = await Effect_exports.runPromise(
    Effect_exports.gen(function* () {
      const hostRepoDir2 = yield* resolveCwd(options.cwd);
      const stateDir2 = resolveStateDir(
        hostRepoDir2,
        options.stateDir ?? DEFAULT_STATE_DIR
      );
      yield* pruneStale(hostRepoDir2, stateDir2).pipe(
        Effect_exports.catchAll(() => Effect_exports.void)
      );
      const { path: worktreePath2 } = yield* create(
        hostRepoDir2,
        stateDir2,
        { branch, baseBranch: options.baseBranch }
      );
      const prepared = yield* Effect_exports.gen(function* () {
        if (options.copyToWorktree && options.copyToWorktree.length > 0 && options.sandbox.tag !== "isolated") {
          yield* copyToWorktree(
            options.copyToWorktree,
            hostRepoDir2,
            worktreePath2,
            options.timeouts?.copyToWorktreeMs
          );
        }
        const onWorktreeReady = options.hooks?.host?.onWorktreeReady;
        if (onWorktreeReady !== void 0 && onWorktreeReady.length > 0) {
          yield* runHostHooks(onWorktreeReady, worktreePath2);
        }
        let providerHandle2;
        let sandbox2;
        let sandboxRepoDir2;
        if (isTestMode) {
          sandbox2 = options._test.buildSandbox(worktreePath2);
          sandboxRepoDir2 = worktreePath2;
          providerHandle2 = options._test.bindMountHandle;
        } else {
          const resolvedEnv = yield* resolveEnv(stateDir2);
          const env = mergeProviderEnv({
            resolvedEnv,
            agentProviderEnv: {},
            sandboxProviderEnv: options.sandbox.env
          });
          const provider = options.sandbox;
          const startResult = yield* (() => {
            if (provider.tag === "isolated") {
              return startSandbox({
                provider,
                hostRepoDir: worktreePath2,
                env,
                copyPaths: options.copyToWorktree,
                copySourceDir: hostRepoDir2
              });
            }
            if (provider.tag === "none") {
              return startSandbox({
                provider,
                hostRepoDir: hostRepoDir2,
                env,
                worktreeOrRepoPath: worktreePath2
              });
            }
            return resolveGitMounts(join(hostRepoDir2, ".git")).pipe(
              Effect_exports.provide(NodeFileSystem_exports.layer),
              Effect_exports.catchAll(() => Effect_exports.succeed([])),
              // Patch git mounts for Windows worktree compatibility (ADR-0006)
              Effect_exports.flatMap(
                (gitMounts) => patchGitMountsForWindows(
                  gitMounts,
                  worktreePath2,
                  SANDBOX_REPO_DIR
                )
              ),
              Effect_exports.tap((gitMounts) => {
                mountsToClean.push(...gitMounts);
              }),
              Effect_exports.flatMap(
                (gitMounts) => startSandbox({
                  provider,
                  hostRepoDir: hostRepoDir2,
                  env,
                  worktreeOrRepoPath: worktreePath2,
                  gitMounts,
                  repoDir: SANDBOX_REPO_DIR
                })
              )
            );
          })();
          providerHandle2 = startResult.handle;
          sandbox2 = startResult.sandbox;
          sandboxRepoDir2 = startResult.worktreePath;
        }
        const sandboxOnReady = options.hooks?.sandbox?.onSandboxReady;
        const hostOnReady = options.hooks?.host?.onSandboxReady;
        if ((sandboxOnReady?.length ?? 0) > 0 || (hostOnReady?.length ?? 0) > 0) {
          yield* Effect_exports.gen(function* () {
            yield* sandbox2.exec(
              `git config --global --add safe.directory "${sandboxRepoDir2}"`
            );
            const sandboxEffects = (sandboxOnReady ?? []).map(
              (hook) => sandbox2.exec(hook.command, {
                cwd: sandboxRepoDir2,
                sudo: hook.sudo
              })
            );
            const allEffects = [
              ...sandboxEffects,
              ...hostOnReady !== void 0 && hostOnReady.length > 0 ? [runHostHooks(hostOnReady, worktreePath2)] : []
            ];
            yield* Effect_exports.all(allEffects, { concurrency: "unbounded" });
          }).pipe(
            Effect_exports.onError(
              () => (providerHandle2 ? Effect_exports.promise(() => providerHandle2.close().catch(() => {
              })) : Effect_exports.void).pipe(Effect_exports.andThen(removeMountTempDirs(mountsToClean)))
            )
          );
        }
        return { providerHandle: providerHandle2, sandbox: sandbox2, sandboxRepoDir: sandboxRepoDir2 };
      }).pipe(
        Effect_exports.onError(
          () => remove(worktreePath2).pipe(
            Effect_exports.catchAll(() => Effect_exports.void),
            Effect_exports.andThen(removeMountTempDirs(mountsToClean))
          )
        )
      );
      return { hostRepoDir: hostRepoDir2, stateDir: stateDir2, worktreePath: worktreePath2, ...prepared };
    }).pipe(Effect_exports.provide(NodeContext_exports.layer))
  );
  const applyToHost = isIsolated && providerHandle ? () => syncOut(worktreePath, providerHandle) : () => Effect_exports.void;
  let closed = false;
  const forceCleanup = () => {
    console.error(`
Worktree preserved at ${worktreePath}`);
    console.error(`  To review: cd ${worktreePath}`);
    console.error(`  To clean up: git worktree remove --force ${worktreePath}`);
  };
  const unregisterShutdown = registerShutdown(forceCleanup);
  const doClose = async () => {
    if (closed) {
      return { preservedWorktreePath: void 0 };
    }
    closed = true;
    return Effect_exports.runPromise(
      Effect_exports.gen(function* () {
        if (providerHandle) {
          yield* Effect_exports.promise(() => providerHandle.close());
        }
        yield* removeMountTempDirs(mountsToClean);
        const isDirty = yield* hasUncommittedChanges(
          worktreePath
        ).pipe(Effect_exports.catchAll(() => Effect_exports.succeed(false)));
        if (isDirty) {
          return { preservedWorktreePath: worktreePath };
        }
        yield* remove(worktreePath).pipe(
          Effect_exports.catchAll(() => Effect_exports.void)
        );
        return { preservedWorktreePath: void 0 };
      })
    );
  };
  const bindMountHandle = options.sandbox.tag === "bind-mount" ? providerHandle : void 0;
  return buildSandboxHandle(
    {
      branch,
      worktreePath,
      hostRepoDir,
      stateDir,
      sandboxRepoDir,
      sandbox,
      providerHandle,
      bindMountHandle,
      providerTag: options.sandbox.tag,
      sandboxProviderEnv: options.sandbox.env,
      applyToHost,
      timeouts: options.timeouts
    },
    async () => {
      unregisterShutdown();
      return doClose();
    }
  );
};
var createWorktree = async (options) => {
  validateTimeoutOptions(options);
  const branch = options.branchStrategy.type === "branch" ? options.branchStrategy.branch : void 0;
  const baseBranch = options.branchStrategy.type === "branch" ? options.branchStrategy.baseBranch : void 0;
  const isMergeToHead = options.branchStrategy.type === "merge-to-head";
  const { hostRepoDir, stateDir, worktreeInfo } = await Effect_exports.gen(
    function* () {
      const repoDir = yield* resolveCwd(options.cwd);
      const stateDir2 = resolveStateDir(
        repoDir,
        options.stateDir ?? DEFAULT_STATE_DIR
      );
      yield* pruneStale(repoDir, stateDir2).pipe(
        Effect_exports.catchAll(() => Effect_exports.void)
      );
      const info = yield* create(repoDir, stateDir2, {
        branch,
        baseBranch
      });
      if (options.copyToWorktree && options.copyToWorktree.length > 0) {
        yield* copyToWorktree(
          options.copyToWorktree,
          repoDir,
          info.path,
          options.timeouts?.copyToWorktreeMs
        );
      }
      const onWorktreeReady = options.hooks?.host?.onWorktreeReady;
      if (onWorktreeReady !== void 0 && onWorktreeReady.length > 0) {
        yield* runHostHooks(onWorktreeReady, info.path);
      }
      return { hostRepoDir: repoDir, stateDir: stateDir2, worktreeInfo: info };
    }
  ).pipe(Effect_exports.provide(NodeContext_exports.layer), Effect_exports.runPromise);
  let closed = false;
  const close = async () => {
    if (closed) {
      return { preservedWorktreePath: void 0 };
    }
    closed = true;
    return Effect_exports.gen(function* () {
      const isDirty = yield* hasUncommittedChanges(
        worktreeInfo.path
      ).pipe(Effect_exports.catchAll(() => Effect_exports.succeed(false)));
      if (isDirty) {
        return { preservedWorktreePath: worktreeInfo.path };
      }
      yield* remove(worktreeInfo.path).pipe(
        Effect_exports.catchAll(() => Effect_exports.void)
      );
      if (isMergeToHead) {
        yield* deleteTempBranchIfUnused(
          hostRepoDir,
          worktreeInfo.branch
        ).pipe(Effect_exports.catchAll(() => Effect_exports.void));
      }
      return { preservedWorktreePath: void 0 };
    }).pipe(Effect_exports.runPromise);
  };
  const worktreeInteractive = async (opts) => {
    opts.signal?.throwIfAborted();
    const { prompt, promptFile, hooks, agent: provider } = opts;
    const resolvedSandbox = opts.sandbox ?? noSandbox();
    const buildInteractiveArgs = provider.buildInteractiveArgs?.bind(provider);
    if (!buildInteractiveArgs) {
      throw new Error(
        `Agent provider "${provider.name}" does not support buildInteractiveArgs, required for interactive sessions.`
      );
    }
    const inner = Effect_exports.gen(function* () {
      const d = yield* Display;
      const hasPromptSource = prompt !== void 0 || promptFile !== void 0;
      const resolved = hasPromptSource ? yield* resolvePrompt({ prompt, promptFile }) : void 0;
      const rawPrompt = resolved?.text ?? "";
      const isInlinePrompt = resolved?.source === "inline";
      const resolvedEnv = yield* resolveEnv(stateDir);
      const env = mergeProviderEnv({
        resolvedEnv,
        agentProviderEnv: provider.env,
        sandboxProviderEnv: resolvedSandbox.env
      });
      const effectiveEnv = { ...env, ...opts.env };
      let substitutedPrompt = rawPrompt;
      if (hasPromptSource && !isInlinePrompt) {
        const userArgs = opts.promptArgs ?? {};
        yield* validateNoBuiltInArgOverride(userArgs);
        const currentHostBranch = yield* getCurrentBranch(hostRepoDir);
        const effectiveArgs = {
          SOURCE_BRANCH: worktreeInfo.branch,
          TARGET_BRANCH: currentHostBranch,
          ...userArgs
        };
        const builtInArgKeysSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
        substitutedPrompt = yield* substitutePromptArgs(
          rawPrompt,
          effectiveArgs,
          builtInArgKeysSet
        );
      } else if (isInlinePrompt) {
        yield* validateNoArgsWithInlinePrompt(opts.promptArgs ?? {});
      }
      yield* d.intro(opts.name ?? "sandcastle interactive");
      yield* d.summary("Interactive Session", {
        Agent: opts.name ?? provider.name,
        Sandbox: resolvedSandbox.name,
        Branch: worktreeInfo.branch
      });
      const mountsToClean = [];
      let handle;
      if (resolvedSandbox.tag === "none") {
        handle = yield* Effect_exports.promise(
          async () => resolvedSandbox.create({
            worktreePath: worktreeInfo.path,
            env: effectiveEnv
          })
        );
      } else if (resolvedSandbox.tag === "isolated") {
        const startResult = yield* d.taskLog(
          "Starting sandbox",
          () => startSandbox({
            provider: resolvedSandbox,
            hostRepoDir: worktreeInfo.path,
            env: effectiveEnv,
            copyPaths: options.copyToWorktree,
            copySourceDir: hostRepoDir
          })
        );
        handle = startResult.handle;
      } else {
        const gitPath = join(hostRepoDir, ".git");
        const rawGitMounts = yield* resolveGitMounts(gitPath);
        const gitMounts = yield* patchGitMountsForWindows(
          rawGitMounts,
          worktreeInfo.path,
          SANDBOX_REPO_DIR
        );
        mountsToClean.push(...gitMounts);
        const startResult = yield* d.taskLog(
          "Starting sandbox",
          () => startSandbox({
            provider: resolvedSandbox,
            hostRepoDir,
            env: effectiveEnv,
            worktreeOrRepoPath: worktreeInfo.path,
            gitMounts,
            repoDir: SANDBOX_REPO_DIR
          })
        );
        handle = startResult.handle;
      }
      return yield* Effect_exports.gen(function* () {
        if (!handle.interactiveExec) {
          throw new Error(
            `Sandbox provider does not support interactiveExec. The provider must implement the optional interactiveExec method to use interactive().`
          );
        }
        const interactiveExecFn = handle.interactiveExec.bind(handle);
        const sandbox = makeSandboxFromHandle(handle);
        const { worktreePath } = handle;
        const applyToHost = resolvedSandbox.tag === "isolated" ? () => syncOut(worktreeInfo.path, handle) : () => Effect_exports.void;
        const lifecycleEffect = withSandboxLifecycle(
          {
            hostRepoDir,
            sandboxRepoDir: worktreePath,
            hooks,
            // merge-to-head: pass `undefined` so the lifecycle records the
            // host's current branch and merges the worktree's commits back into
            // it. branch strategy: pin to the worktree's branch.
            branch: isMergeToHead ? void 0 : worktreeInfo.branch,
            hostWorktreePath: worktreeInfo.path,
            applyToHost,
            timeouts: options.timeouts,
            keepSourceBranch: isMergeToHead
          },
          sandbox,
          (ctx) => Effect_exports.gen(function* () {
            const fullPrompt = !hasPromptSource || isInlinePrompt ? substitutedPrompt : yield* preprocessPrompt(
              substitutedPrompt,
              ctx.sandbox,
              ctx.sandboxRepoDir
            );
            const interactiveArgs = buildInteractiveArgs({
              prompt: fullPrompt,
              dangerouslySkipPermissions: resolvedSandbox.tag !== "none"
            });
            const result = yield* raceAbortSignal(
              Effect_exports.promise(
                async () => interactiveExecFn(interactiveArgs, {
                  stdin: process.stdin,
                  stdout: process.stdout,
                  stderr: process.stderr,
                  cwd: worktreePath
                })
              ),
              opts.signal
            );
            return result.exitCode;
          })
        );
        const lifecycleResult = yield* lifecycleEffect;
        const exitCode = lifecycleResult.result;
        yield* d.summary("Session Complete", {
          Commits: String(lifecycleResult.commits.length),
          Branch: lifecycleResult.branch,
          "Exit code": String(exitCode)
        });
        return {
          commits: lifecycleResult.commits,
          branch: lifecycleResult.branch,
          preservedWorktreePath: void 0,
          exitCode
        };
      }).pipe(
        // Always close sandbox handle
        Effect_exports.ensuring(
          Effect_exports.promise(async () => handle.close().catch(() => {
          })).pipe(
            Effect_exports.andThen(removeMountTempDirs(mountsToClean))
          )
        )
      );
    });
    try {
      return await Effect_exports.runPromise(
        inner.pipe(
          Effect_exports.provide(
            Layer_exports.mergeAll(
              ClackDisplay.layer,
              NodeContext_exports.layer,
              NodeFileSystem_exports.layer
            )
          )
        )
      );
    } catch (error) {
      opts.signal?.throwIfAborted();
      throw error;
    }
  };
  const worktreeRun = async (opts) => {
    opts.signal?.throwIfAborted();
    const { prompt, promptFile, hooks, agent: provider } = opts;
    const sandboxProvider = opts.sandbox;
    const maxIterations = opts.maxIterations ?? 1;
    validateTimeoutOptions(opts);
    const isResuming = opts.resumeSession !== void 0 && opts.resumeSession !== "";
    if (isResuming && maxIterations > 1) {
      throw new Error(
        "resumeSession cannot be combined with maxIterations > 1. Resume applies to iteration 1 only; multi-iteration resume semantics are not supported."
      );
    }
    if (isResuming) {
      await assertResumeSessionExists({
        provider,
        sandboxTag: sandboxProvider.tag,
        hostRepoDir,
        resumeSession: opts.resumeSession
      });
    }
    const inner = Effect_exports.gen(function* () {
      const resolved = yield* resolvePrompt({ prompt, promptFile });
      const rawPrompt = resolved.text;
      const isInlinePrompt = resolved.source === "inline";
      const resolvedEnv = yield* resolveEnv(stateDir);
      const env = mergeProviderEnv({
        resolvedEnv,
        agentProviderEnv: provider.env,
        sandboxProviderEnv: sandboxProvider.env
      });
      const effectiveEnv = { ...env, ...opts.env };
      const userArgs = opts.promptArgs ?? {};
      let resolvedPrompt;
      if (isInlinePrompt) {
        yield* validateNoArgsWithInlinePrompt(userArgs);
        resolvedPrompt = rawPrompt;
      } else {
        yield* validateNoBuiltInArgOverride(userArgs);
        const currentHostBranch = yield* getCurrentBranch(hostRepoDir);
        const effectiveArgs = {
          SOURCE_BRANCH: worktreeInfo.branch,
          TARGET_BRANCH: currentHostBranch,
          ...userArgs
        };
        const builtInArgKeysSet = new Set(BUILT_IN_PROMPT_ARG_KEYS);
        resolvedPrompt = yield* substitutePromptArgs(
          rawPrompt,
          effectiveArgs,
          builtInArgKeysSet
        );
      }
      const mountsToClean = [];
      let handle;
      let sandboxRepoDir;
      if (sandboxProvider.tag === "isolated") {
        const startResult = yield* startSandbox({
          provider: sandboxProvider,
          hostRepoDir: worktreeInfo.path,
          env: effectiveEnv,
          copyPaths: options.copyToWorktree,
          copySourceDir: hostRepoDir
        });
        handle = startResult.handle;
        sandboxRepoDir = startResult.worktreePath;
      } else if (sandboxProvider.tag === "none") {
        const startResult = yield* startSandbox({
          provider: sandboxProvider,
          hostRepoDir,
          env: effectiveEnv,
          worktreeOrRepoPath: worktreeInfo.path
        });
        handle = startResult.handle;
        sandboxRepoDir = startResult.worktreePath;
      } else {
        const gitPath = join(hostRepoDir, ".git");
        const rawGitMounts = yield* resolveGitMounts(gitPath);
        const gitMounts = yield* patchGitMountsForWindows(
          rawGitMounts,
          worktreeInfo.path,
          SANDBOX_REPO_DIR
        );
        mountsToClean.push(...gitMounts);
        const startResult = yield* startSandbox({
          provider: sandboxProvider,
          hostRepoDir,
          env: effectiveEnv,
          worktreeOrRepoPath: worktreeInfo.path,
          gitMounts,
          repoDir: SANDBOX_REPO_DIR
        });
        handle = startResult.handle;
        sandboxRepoDir = startResult.worktreePath;
      }
      const sandbox = makeSandboxFromHandle(handle);
      const applyToHost = sandboxProvider.tag === "isolated" ? () => syncOut(worktreeInfo.path, handle) : () => Effect_exports.void;
      const resolvedLogging = opts.logging ?? {
        type: "file",
        path: join(
          stateDir,
          "logs",
          buildLogFilename(worktreeInfo.branch, void 0, opts.name)
        )
      };
      const runDisplayLayer = resolvedLogging.type === "file" ? (() => {
        printFileDisplayStartup({
          logPath: resolvedLogging.path,
          agentName: opts.name,
          branch: worktreeInfo.branch
        });
        return Layer_exports.provide(
          FileDisplay.layer(resolvedLogging.path),
          NodeFileSystem_exports.layer
        );
      })() : ClackDisplay.layer;
      const bindMountHandle = sandboxProvider.tag === "bind-mount" ? handle : void 0;
      const reuseFactoryLayer = reuseSandboxFactoryLayer(
        {
          hostWorktreePath: worktreeInfo.path,
          sandboxRepoPath: sandboxRepoDir,
          applyToHost,
          bindMountHandle
        },
        sandbox
      );
      const streamEmitterLayer = agentStreamEmitterLayer(
        buildAgentStreamHandler(resolvedLogging)
      );
      const runLayer = Layer_exports.mergeAll(
        reuseFactoryLayer,
        runDisplayLayer,
        streamEmitterLayer
      );
      const result = yield* Effect_exports.gen(function* () {
        const display = yield* Display;
        yield* display.intro(opts.name ?? "sandcastle");
        const orchestrateResult = yield* orchestrate({
          hostRepoDir,
          iterations: maxIterations,
          hooks,
          prompt: resolvedPrompt,
          // merge-to-head: pass `undefined` so the lifecycle records the host's
          // current branch and routes through the merge step. branch strategy:
          // pin to the worktree's branch so commits stay there.
          branch: isMergeToHead ? void 0 : worktreeInfo.branch,
          provider,
          completionSignal: opts.completionSignal,
          idleTimeoutSeconds: opts.idleTimeoutSeconds,
          completionTimeoutSeconds: opts.completionTimeoutSeconds,
          name: opts.name,
          resumeSession: opts.resumeSession,
          signal: opts.signal,
          skipPromptExpansion: isInlinePrompt,
          timeouts: options.timeouts,
          keepSourceBranch: isMergeToHead
        });
        const completion = buildCompletionMessage(
          orchestrateResult.completionSignal,
          orchestrateResult.iterations.length
        );
        yield* display.status(completion.message, completion.severity);
        for (const line of buildContextWindowLines(
          orchestrateResult.iterations
        )) {
          yield* display.text(line);
        }
        return orchestrateResult;
      }).pipe(
        Effect_exports.provide(runLayer),
        // Always close sandbox handle
        Effect_exports.ensuring(
          Effect_exports.promise(async () => handle.close().catch(() => {
          })).pipe(
            Effect_exports.andThen(removeMountTempDirs(mountsToClean))
          )
        )
      );
      return {
        iterations: result.iterations,
        completionSignal: result.completionSignal,
        stdout: result.stdout,
        commits: result.commits,
        branch: result.branch,
        logFilePath: resolvedLogging.type === "file" ? resolvedLogging.path : void 0
      };
    });
    try {
      return await Effect_exports.runPromise(
        inner.pipe(
          Effect_exports.provide(
            Layer_exports.mergeAll(
              ClackDisplay.layer,
              NodeContext_exports.layer,
              NodeFileSystem_exports.layer
            )
          )
        )
      );
    } catch (error) {
      opts.signal?.throwIfAborted();
      throw error;
    }
  };
  const worktreeCreateSandbox = async (opts) => createSandboxFromWorktree({
    branch: worktreeInfo.branch,
    worktreePath: worktreeInfo.path,
    hostRepoDir,
    stateDir,
    sandbox: opts.sandbox,
    hooks: opts.hooks,
    // Isolated sandboxes sync in via git bundle, so the worktree's own
    // copyToWorktree files (often gitignored) must be copied in as well.
    copyToWorktree: opts.sandbox.tag === "isolated" ? [...options.copyToWorktree ?? [], ...opts.copyToWorktree ?? []] : opts.copyToWorktree,
    timeouts: opts.timeouts,
    branchStrategy: options.branchStrategy,
    _test: opts._test
  });
  return {
    branch: worktreeInfo.branch,
    worktreePath: worktreeInfo.path,
    run: worktreeRun,
    interactive: worktreeInteractive,
    createSandbox: worktreeCreateSandbox,
    close,
    async [Symbol.asyncDispose]() {
      await close();
    }
  };
};

// src/CwdError.ts
var CwdError2 = CwdError;

// src/SyncOutPathError.ts
var SyncOutPathError2 = SyncOutPathError;

export { CwdError2 as CwdError, Output, StructuredOutputError, SyncOutPathError2 as SyncOutPathError, claudeCode, claudeHostSessionPath, claudeSandboxSessionPath, codex, copilot, createSandbox, createWorktree, cursor, encodeProjectPath, findClaudeSessionOnHost, findCodexSessionOnHost, interactive, opencode, pi, run, transferClaudeSession, transferCodexSession };
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map