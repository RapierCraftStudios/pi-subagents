import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Compile } from "typebox/compile";
import { SubagentParams } from "../../src/extension/schemas.ts";
import { resolveMaxSubagentSpawnsPerRun } from "../../src/shared/types.ts";
import { runWorkflowScript } from "../../src/workflows/scripted-workflow.ts";

type CapacitySchema = {
	type?: string;
	minimum?: number;
	maximum?: number;
	description?: string;
};

function capacitySchema(name: "globalConcurrencyLimit" | "maxSubagentSpawnsPerRun"): CapacitySchema {
	// SAFETY: Type.Object returns a schema object with the declared top-level properties.
	const properties = SubagentParams.properties as Record<string, CapacitySchema>;
	return properties[name] ?? {};
}

describe("workflow capacity schema", () => {
	it("keeps the omitted fan-out default at 64", () => {
		assert.equal(resolveMaxSubagentSpawnsPerRun(undefined), 64);
	});

	it("keeps capacity controls out of child run calls", async () => {
		let launches = 0;
		for (const field of ["globalConcurrencyLimit", "maxSubagentSpawnsPerRun"] as const) {
			await assert.rejects(
				runWorkflowScript({
					script: `return runs.run("child", { agent: "worker", task: "run", ${field}: 2 });`,
					async launch() { launches += 1; return { key: "child", ok: true, output: "unexpected", artifactPaths: [] }; },
					async status() { return { key: "child", ok: true, output: "unused", artifactPaths: [] }; },
				}),
				/runs\.run accepts one child via .*execution controls only/,
			);
		}
		assert.equal(launches, 0);
	});

	it("exposes positive safe-integer workflow-only capacity overrides", () => {
		const global = capacitySchema("globalConcurrencyLimit");
		assert.equal(global.type, "integer");
		assert.equal(global.minimum, 1);
		assert.equal(global.maximum, undefined);
		assert.equal(global.description, undefined);

		const fanout = capacitySchema("maxSubagentSpawnsPerRun");
		assert.equal(fanout.type, "integer");
		assert.equal(fanout.minimum, 1);
		assert.equal(fanout.maximum, undefined);
		assert.equal(fanout.description, undefined);
	});

	it("rejects zero and fractional override values in the provider schema", () => {
		const validator = Compile(SubagentParams);
		assert.equal(validator.Check({ workflowScript: "return 1", globalConcurrencyLimit: 1, maxSubagentSpawnsPerRun: Number.MAX_SAFE_INTEGER }), true);
		for (const value of [0, -1, 1.5]) {
			assert.equal(validator.Check({ workflowScript: "return 1", globalConcurrencyLimit: value }), false, `globalConcurrencyLimit=${String(value)}`);
			assert.equal(validator.Check({ workflowScript: "return 1", maxSubagentSpawnsPerRun: value }), false, `maxSubagentSpawnsPerRun=${String(value)}`);
		}
	});
});
