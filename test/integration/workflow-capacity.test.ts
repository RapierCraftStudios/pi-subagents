import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { createEventBus, createMockPi, createTempDir, makeAgent, makeMinimalCtx, removeTempDir, tryImport, type MockPi } from "../support/helpers.ts";
import type { ExtensionConfig, SubagentState } from "../../src/shared/types.ts";
import { DIRS } from "../../src/shared/types.ts";
import { createRunFanoutBudget } from "../../src/runs/shared/run-fanout-budget.ts";

type WorkflowResult = { state?: string };
type WorkflowStatus = { steps?: Array<{ status?: string }> };
type ExecutorModule = typeof import("../../src/runs/foreground/subagent-executor.ts");

const executorModule = await tryImport<ExecutorModule>("./src/runs/foreground/subagent-executor.ts");
const available = executorModule?.createSubagentExecutor !== undefined;

describe("workflow capacity overrides", { skip: !available ? "executor not importable" : undefined }, () => {
	let mockPi: MockPi;
	let tempDir: string;
	let agentDir: string;
	let previousAgentDir: string | undefined;

	before(() => {
		mockPi = createMockPi();
		mockPi.install();
	});

	after(() => mockPi.uninstall());

	beforeEach(() => {
		tempDir = createTempDir("pi-workflow-capacity-");
		agentDir = createTempDir("pi-workflow-capacity-agent-");
		previousAgentDir = process.env.PI_CODING_AGENT_DIR;
		process.env.PI_CODING_AGENT_DIR = agentDir;
		mockPi.reset();
	});

	function makeExecutor(config: ExtensionConfig = {}, asyncByDefault = false) {
		const createExecutor = executorModule!.createSubagentExecutor;
		return createExecutor({
			pi: { events: createEventBus(), getSessionName: () => undefined },
			// SAFETY: The executor only reads these initialized state maps in this test.
			state: {
				baseCwd: tempDir,
				currentSessionId: null,
				asyncJobs: new Map(),
				foregroundControls: new Map(),
				lastForegroundControlId: null,
			} as SubagentState,
			config,
			asyncByDefault,
			tempArtifactsDir: tempDir,
			getSubagentSessionRoot: () => path.join(tempDir, ".pi", "subagents", "sessions"),
			expandTilde: (value: string) => value,
			discoverAgents: () => ({ agents: [makeAgent("echo")] }),
		});
	}

	async function waitForWorkflowResult(runId: string): Promise<WorkflowResult> {
		const resultPath = path.join(DIRS.results, `${runId}.json`);
		for (let attempt = 0; attempt < 250; attempt += 1) {
			if (fs.existsSync(resultPath)) {
				// SAFETY: The workflow result file is written by the executor as a JSON object.
				return JSON.parse(fs.readFileSync(resultPath, "utf8")) as WorkflowResult;
			}
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		assert.fail(`timed out waiting for workflow result ${runId}`);
	}

	afterEach(() => {
		if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
		removeTempDir(agentDir);
		removeTempDir(tempDir);
	});

	it("lets an explicit foreground workflow fan-out cap override config", async () => {
		mockPi.onCall({ output: "one" });
		mockPi.onCall({ output: "two" });
		const result = await makeExecutor({ maxSubagentSpawnsPerRun: 1 }).executePublic(
			"workflow-capacity-foreground",
			{ workflowScript: `return await runs.all([{ key: "one", agent: "echo", task: "one" }, { key: "two", agent: "echo", task: "two" }]);`, async: false, maxSubagentSpawnsPerRun: 2 },
			new AbortController().signal,
			undefined,
			makeMinimalCtx(tempDir),
		);
		assert.equal(result.isError, undefined, result.content[0]?.text ?? "workflow failed");
		assert.equal(mockPi.callCount(), 2);
	});

	it("keeps an inherited nested fan-out descriptor authoritative over a child override", async () => {
		const inherited = createRunFanoutBudget(`workflow-capacity-inherited-${Date.now()}`, 1);
		try {
			const result = await makeExecutor().execute(
				"workflow-capacity-inherited",
				{ workflowScript: `return await runs.all([{ key: "one", agent: "echo", task: "one" }, { key: "two", agent: "echo", task: "two" }]);`, async: false, maxSubagentSpawnsPerRun: 2, runFanoutBudget: inherited },
				new AbortController().signal,
				undefined,
				makeMinimalCtx(tempDir),
			);
			assert.equal(result.isError, undefined, result.content[0]?.text ?? "workflow failed");
			assert.match(result.content[0]?.text ?? "", /Run fan-out limit reached/);
			// SAFETY: runs.all returns the ordered child result array for this workflow script.
			const children = result.details.workflow?.value as Array<{ ok?: boolean }>;
			assert.deepEqual(children.map((child) => child.ok), [false, false]);
			assert.equal(mockPi.callCount(), 0);
		} finally {
			fs.rmSync(inherited.directory, { recursive: true, force: true });
		}
	});

	it("uses explicit async workflow capacity and does not serialize a raised global cap", async () => {
		mockPi.onCall({ delay: 2_000, output: "one" });
		mockPi.onCall({ delay: 2_000, output: "two" });
		const started = await makeExecutor({ globalConcurrencyLimit: 1, maxSubagentSpawnsPerRun: 1 }).executePublic(
			"workflow-capacity-async",
			{ workflowScript: `return await runs.all([{ key: "one", agent: "echo", task: "one" }, { key: "two", agent: "echo", task: "two" }]);`, async: true, globalConcurrencyLimit: 2, maxSubagentSpawnsPerRun: 2 },
			new AbortController().signal,
			undefined,
			makeMinimalCtx(tempDir),
		);
		assert.equal(started.isError, undefined, started.content[0]?.text ?? "workflow failed to start");
		const runId = started.details.asyncId;
		assert.ok(runId);
		const statusPath = path.join(started.details.asyncDir!, "status.json");
		let sawTwoRunning = false;
		for (let attempt = 0; attempt < 100; attempt += 1) {
			if (fs.existsSync(statusPath)) {
				// SAFETY: status.json is the executor's persisted async status object.
				const status = JSON.parse(fs.readFileSync(statusPath, "utf8")) as WorkflowStatus;
				if ((status.steps ?? []).filter((step) => step.status === "running").length === 2) {
					sawTwoRunning = true;
					break;
				}
			}
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		assert.equal(sawTwoRunning, true, "explicit globalConcurrencyLimit should allow both children to run");
		const persisted = await waitForWorkflowResult(runId);
		assert.equal(persisted.state, "complete");
		fs.rmSync(started.details.asyncDir!, { recursive: true, force: true });
		fs.rmSync(path.join(DIRS.results, `${runId}.json`), { force: true });
	});
});
