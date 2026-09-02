const name = "dsh-genbio-remote-openviking";
const inject = ["genbioRemote", "openvikingMemory"];

function apply(ctx) {
  const genbioRemote = ctx.get("genbioRemote");
  if (!genbioRemote) return;
  const initial = ctx.get("openvikingMemory");
  if (!initial || typeof initial.publishExternalRecord !== "function") {
    ctx.logger?.warn?.("genbio OpenViking adapter: publishExternalRecord is unavailable");
    return;
  }
  const dispose = genbioRemote.registerMemoryPublisher({
    publish(session, record) {
      const openvikingMemory = ctx.get("openvikingMemory");
      if (!openvikingMemory || typeof openvikingMemory.publishExternalRecord !== "function") {
        return { ok: false, status: "unavailable", error: "OpenViking publisher is unavailable" };
      }
      return openvikingMemory.publishExternalRecord(session, record);
    },
  });
  ctx.effect(() => dispose, "genbio OpenViking publisher");
}

export { apply, inject, name };
