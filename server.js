const express = require("express");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const RAILWAY_API = "https://backboard.railway.app/graphql/v2";
const REPO = "arvin341az-glitch/railway-x3ui";
const TARGET_PORT = 2053;

async function gql(token, query, variables) {
  const res = await fetch(RAILWAY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer " + token,
    },
    body: JSON.stringify({ query, variables }),
  });
  const data = await res.json();
  if (data.errors) {
    throw new Error(data.errors.map((e) => e.message).join("; "));
  }
  return data.data;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// SSE endpoint - does the whole deployment and streams progress
// Token is only ever held in memory for the duration of this single request.
app.post("/api/deploy", async (req, res) => {
  const { token, projectName } = req.body;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = (event, payload) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
  };

  if (!token) {
    send("error", { message: "توکن ارسال نشده" });
    return res.end();
  }

  try {
    send("step", { id: "auth", status: "active" });
    await gql(token, `query { me { id name email } }`);
    send("step", { id: "auth", status: "done" });

    send("step", { id: "project", status: "active" });
    const projData = await gql(
      token,
      `mutation($input: ProjectCreateInput!) {
        projectCreate(input: $input) { id name }
      }`,
      { input: { name: projectName || "x-ui-panel" } }
    );
    const projectId = projData.projectCreate.id;
    send("step", { id: "project", status: "done" });

    const envData = await gql(
      token,
      `query($id: String!) {
        project(id: $id) { environments { edges { node { id name } } } }
      }`,
      { id: projectId }
    );
    const environmentId = envData.project.environments.edges[0].node.id;

    send("step", { id: "service", status: "active" });
    const svcData = await gql(
      token,
      `mutation($input: ServiceCreateInput!) {
        serviceCreate(input: $input) { id name }
      }`,
      { input: { projectId, source: { repo: REPO } } }
    );
    const serviceId = svcData.serviceCreate.id;
    send("step", { id: "service", status: "done" });

    send("step", { id: "vars", status: "active" });
    await gql(
      token,
      `mutation($input: VariableUpsertInput!) { variableUpsert(input: $input) }`,
      {
        input: {
          projectId,
          environmentId,
          serviceId,
          name: "PORT",
          value: String(TARGET_PORT),
        },
      }
    );
    send("step", { id: "vars", status: "done" });

    send("step", { id: "deploy", status: "active" });
    let status = null;
    for (let i = 0; i < 40; i++) {
      await sleep(6000);
      const depData = await gql(
        token,
        `query($input: DeploymentsInput!) {
          deployments(input: $input, first: 1) {
            edges { node { id status } }
          }
        }`,
        { input: { projectId, serviceId, environmentId } }
      );
      const edges = depData.deployments.edges;
      if (edges.length > 0) {
        status = edges[0].node.status;
        if (status === "SUCCESS") break;
        if (status === "FAILED" || status === "CRASHED") {
          throw new Error("دیپلوی با خطا مواجه شد: " + status);
        }
      }
    }
    if (status !== "SUCCESS") {
      throw new Error(
        "دیپلوی بیش از حد انتظار طول کشید. از داشبورد Railway وضعیت رو چک کنید."
      );
    }
    send("step", { id: "deploy", status: "done" });

    send("step", { id: "domain", status: "active" });
    const domainData = await gql(
      token,
      `mutation($input: ServiceDomainCreateInput!) {
        serviceDomainCreate(input: $input) { domain }
      }`,
      { input: { environmentId, serviceId, targetPort: TARGET_PORT } }
    );
    const domain = domainData.serviceDomainCreate.domain;
    send("step", { id: "domain", status: "done" });

    send("complete", { url: "https://" + domain + "/" });
  } catch (err) {
    send("error", { message: err.message || String(err) });
  } finally {
    res.end();
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
