import { useCallback, useEffect, useRef, useState } from "react";

const emptyDraft = {
  id: "",
  cpu_units_per_tick: "10",
  mem_mb: "1024",
  rate_limit_per_sec: "1",
};

const defaultAutoscale = {
  scale_up_cpu_percent: "80",
  scale_down_cpu_percent: "30",
  queue_threshold: "1",
  error_rate_threshold_percent: "1",
  cooldown_ticks: "2",
  min_cpu_percent: "50",
  max_cpu_percent: "200",
};

const defaultQueuePolicy = {
  max_queue_length: "",
  max_wait_ticks: "",
  starvation_guard_ticks: "",
};

const strategyLabels = {
  projected_drain: "Projected drain time",
  least_loaded: "Least loaded server",
  round_robin: "Round robin",
};

const dropReasonLabels = {
  no_capacity: "no server fits",
  queue_full: "queue full",
  wait_timeout: "waited too long",
};

// Large runs would render thousands of SVG nodes; show the earliest ones only.
const MAX_TIMELINE_BARS = 2000;
const MAX_DROPPED_LABELS = 40;
const SERVER_ID_PATTERN = "[A-Za-z0-9][A-Za-z0-9._\\-]*";

const chartColors = ["#4568e8", "#1b9a79", "#d78a35", "#9b64c7", "#dc5d6b", "#399db3"];

async function api(path, options = {}) {
  const response = await fetch(path, options);
  if (response.status === 204) return null;
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.detail || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return payload;
}

function formatWait(value) {
  return value === null || value === undefined ? "—" : `${Number(value).toFixed(2)} ticks`;
}

function formatPercent(value) {
  return `${Number(value || 0).toFixed(1)}%`;
}

function SeriesChart({ title, points, lines, maxValue = 100, suffix = "%" }) {
  if (!points?.length) return <div className="empty-state chart-empty">No samples are available for this run.</div>;
  const width = 1000;
  const height = 250;
  const left = 54;
  const right = 20;
  const top = 18;
  const bottom = 32;
  const innerWidth = width - left - right;
  const innerHeight = height - top - bottom;
  const minTick = points.reduce((value, point) => Math.min(value, point.tick), Number.POSITIVE_INFINITY);
  const maxTick = points.reduce((value, point) => Math.max(value, point.tick), minTick + 1);
  const yMax = Math.max(1, maxValue);
  const xAt = (tick) => left + ((tick - minTick) / (maxTick - minTick)) * innerWidth;
  const yAt = (value) => top + (1 - Math.max(0, Math.min(yMax, Number(value || 0)) / yMax)) * innerHeight;
  const ticks = [0, .25, .5, .75, 1];

  return (
    <div className="chart-block">
      <div className="chart-title-row"><h4>{title}</h4><span>Tick</span></div>
      <div className="chart-scroll">
        <svg className="metric-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
          {ticks.map((fraction) => {
            const value = yMax * fraction;
            const y = yAt(value);
            return (
              <g key={fraction}>
                <line x1={left} y1={y} x2={width - right} y2={y} className="chart-gridline" />
                <text x={left - 9} y={y + 3} textAnchor="end" className="chart-axis-label">
                  {`${Number(value.toFixed(1))}${suffix}`}
                </text>
              </g>
            );
          })}
          <text x={left} y={height - 8} className="chart-axis-label">{minTick}</text>
          <text x={width - right} y={height - 8} textAnchor="end" className="chart-axis-label">{maxTick}</text>
          {lines.map((line, lineIndex) => {
            const values = line.points || points;
            const pointMap = new Map(values.map((point) => [point.tick, point[line.key]]));
            const coordinates = points
              .filter((point) => pointMap.has(point.tick))
              .map((point) => `${xAt(point.tick)},${yAt(pointMap.get(point.tick))}`)
              .join(" ");
            return (
              <polyline
                key={line.name}
                points={coordinates}
                fill="none"
                stroke={line.color || chartColors[lineIndex % chartColors.length]}
                strokeWidth="3"
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            );
          })}
        </svg>
      </div>
      <div className="chart-legend">
        {lines.map((line, index) => (
          <span key={line.name}><i style={{ background: line.color || chartColors[index % chartColors.length] }} />{line.name}</span>
        ))}
      </div>
    </div>
  );
}

function RequestTimeline({ events, metrics }) {
  if (!events?.length || !metrics?.server_series) {
    return <div className="empty-state">Run a workload to see request start and finish times by server.</div>;
  }
  const servers = Object.keys(metrics.server_series).sort();
  const arrivals = new Map();
  const starts = new Map();
  const finishes = new Map();
  const dropped = [];
  for (const event of events) {
    if (event.event === "REQUEST_ARRIVED") arrivals.set(event.request_id, event.t);
    if (event.event === "REQUEST_STARTED") starts.set(event.request_id, event);
    if (event.event === "REQUEST_FINISHED") finishes.set(event.request_id, event);
    if (event.event === "REQUEST_DROPPED") dropped.push(event);
  }
  const allBars = [...starts.entries()].map(([requestId, start]) => ({
    requestId,
    serverId: start.server_id,
    start: start.t,
    finish: finishes.get(requestId)?.t ?? start.t + 1,
    arrival: arrivals.get(requestId),
  }));
  allBars.sort((a, b) => a.start - b.start || a.requestId.localeCompare(b.requestId));
  const bars = allBars.slice(0, MAX_TIMELINE_BARS);
  const visibleTicks = bars.length
    ? bars.flatMap((bar) => [bar.arrival ?? bar.start, bar.finish])
    : events.map((event) => event.t);
  const minTick = visibleTicks.reduce((value, tick) => Math.min(value, tick), Number.POSITIVE_INFINITY);
  const maxTick = visibleTicks.reduce((value, tick) => Math.max(value, tick), minTick + 1);
  const width = 1000;
  const labelWidth = 120;
  const right = 18;
  const plotWidth = width - labelWidth - right;
  const laneHeight = 58;
  const height = 54 + Math.max(1, servers.length) * laneHeight;
  const xAt = (tick) => labelWidth + ((tick - minTick) / (maxTick - minTick)) * plotWidth;
  const colorFor = (value) => {
    let code = 0;
    for (const char of value) code = (code * 31 + char.charCodeAt(0)) >>> 0;
    return chartColors[code % chartColors.length];
  };

  return (
    <>
      <div className="timeline-scroll">
        <svg className="request-timeline" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Request execution timeline">
          <line x1={labelWidth} y1="30" x2={width - right} y2="30" className="timeline-axis" />
          <text x={labelWidth} y="18" className="timeline-tick-label">Tick {minTick}</text>
          <text x={width - right} y="18" textAnchor="end" className="timeline-tick-label">Tick {maxTick}</text>
          {servers.map((serverId, serverIndex) => {
            const y = 44 + serverIndex * laneHeight;
            return (
              <g key={serverId}>
                <rect x="0" y={y - 4} width={width} height={laneHeight - 1} rx="5" className="timeline-lane" />
                <text x="8" y={y + 17} className="timeline-server-label">{serverId}</text>
                <line x1={labelWidth} y1={y + 27} x2={width - right} y2={y + 27} className="timeline-lane-line" />
                {bars.filter((bar) => bar.serverId === serverId).map((bar) => {
                  const x = xAt(bar.start);
                  const end = xAt(bar.finish);
                  const barWidth = Math.max(8, end - x);
                  return (
                    <g key={bar.requestId}>
                      {bar.arrival !== undefined && bar.arrival < bar.start && (
                        <line x1={xAt(bar.arrival)} y1={y + 22} x2={x} y2={y + 22} className="queue-wait-line" />
                      )}
                      <rect x={x} y={y + 10} width={barWidth} height="24" rx="5" fill={colorFor(bar.requestId)} />
                      {barWidth > 46 && <text x={x + 7} y={y + 26} className="timeline-request-label">{bar.requestId}</text>}
                      <title>{`${bar.requestId}: starts at ${bar.start}, finishes at ${bar.finish}`}</title>
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
      {dropped.length > 0 && (
        <div className="dropped-row">
          <strong>Dropped</strong>
          {dropped.slice(0, MAX_DROPPED_LABELS).map((event) => <span key={event.request_id}>{event.request_id} · tick {event.t}</span>)}
          {dropped.length > MAX_DROPPED_LABELS && <span>and {(dropped.length - MAX_DROPPED_LABELS).toLocaleString()} more</span>}
        </div>
      )}
      <p className="chart-note">
        Bars show execution intervals. A faint line before a bar shows queue wait time.
        {allBars.length > bars.length && ` Showing the first ${bars.length.toLocaleString()} of ${allBars.length.toLocaleString()} started requests.`}
      </p>
    </>
  );
}

export default function App() {
  const [servers, setServers] = useState([]);
  const [draft, setDraft] = useState(emptyDraft);
  const [editingId, setEditingId] = useState(null);
  const [selectedFile, setSelectedFile] = useState(null);
  const [allowConcurrency, setAllowConcurrency] = useState(false);
  const [strategy, setStrategy] = useState("projected_drain");
  const [strategies, setStrategies] = useState(Object.keys(strategyLabels));
  const [enableAutoscaling, setEnableAutoscaling] = useState(false);
  const [autoscaleSettings, setAutoscaleSettings] = useState(defaultAutoscale);
  const [enableQueuePolicy, setEnableQueuePolicy] = useState(false);
  const [queuePolicy, setQueuePolicy] = useState(defaultQueuePolicy);
  const importInputRef = useRef(null);
  const [runDetails, setRunDetails] = useState({ metrics: null, events: [] });
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [detailsError, setDetailsError] = useState("");
  const fileInputRef = useRef(null);
  const [latest, setLatest] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const loadRunDetails = useCallback(async (runId) => {
    if (!runId) {
      setRunDetails({ metrics: null, events: [] });
      return;
    }
    setDetailsLoading(true);
    setDetailsError("");
    try {
      const [metrics, traceResponse] = await Promise.all([
        api(`/api/simulations/${runId}/metrics`),
        fetch(`/api/simulations/${runId}/trace`),
      ]);
      if (!traceResponse.ok) throw new Error(`Could not load trace (${traceResponse.status})`);
      const rawTrace = await traceResponse.text();
      const events = rawTrace.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
      setRunDetails({ metrics, events });
    } catch (reason) {
      setRunDetails({ metrics: null, events: [] });
      setDetailsError(reason.message);
    } finally {
      setDetailsLoading(false);
    }
  }, []);

  // Server edits only change the server list, so they reload just that.
  const loadServers = useCallback(async () => {
    const configuration = await api("/api/configuration");
    setServers(configuration.servers);
    if (configuration.scheduling_strategies?.length) setStrategies(configuration.scheduling_strategies);
  }, []);

  const refresh = useCallback(async () => {
    setError("");
    try {
      const [, runResult] = await Promise.all([
        loadServers(),
        api("/api/simulations/latest").catch((reason) => {
          if (reason.status === 404) return null;
          throw reason;
        }),
      ]);
      setLatest(runResult);
      await loadRunDetails(runResult?.run_id);
    } catch (reason) {
      setError(reason.message);
    } finally {
      setLoading(false);
    }
  }, [loadRunDetails, loadServers]);

  useEffect(() => { refresh(); }, [refresh]);

  function updateDraft(field, value) {
    setDraft((current) => ({ ...current, [field]: value }));
  }

  function updateAutoscale(field, value) {
    setAutoscaleSettings((current) => ({ ...current, [field]: value }));
  }

  function updateQueuePolicy(field, value) {
    setQueuePolicy((current) => ({ ...current, [field]: value }));
  }

  function startEdit(server) {
    setEditingId(server.id);
    setDraft({
      id: server.id,
      cpu_units_per_tick: String(server.cpu_units_per_tick),
      mem_mb: String(server.mem_mb),
      rate_limit_per_sec: String(server.rate_limit_per_sec),
    });
    setNotice("");
  }

  function resetForm() {
    setEditingId(null);
    setDraft(emptyDraft);
  }

  async function saveServer(event) {
    event.preventDefault();
    setError("");
    setNotice("");
    const numericValues = [Number(draft.cpu_units_per_tick), Number(draft.mem_mb), Number(draft.rate_limit_per_sec)];
    if (numericValues.some((value) => !Number.isSafeInteger(value)) || numericValues[0] <= 0 || numericValues[1] <= 0 || numericValues[2] < 0) {
      setError("CPU and memory must be positive whole numbers; rate limit must be a non-negative whole number.");
      return;
    }
    const body = {
      id: draft.id.trim(),
      cpu_units_per_tick: numericValues[0],
      mem_mb: numericValues[1],
      rate_limit_per_sec: numericValues[2],
    };
    if (!body.id) { setError("Enter a server ID."); return; }

    setSaving(true);
    try {
      if (editingId) {
        await api(`/api/servers/${encodeURIComponent(editingId)}`, {
          method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
        });
        setNotice(`Server ${editingId} updated. Changes apply to future runs.`);
      } else {
        await api("/api/servers", {
          method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
        });
        setNotice(`Server ${body.id} added. It is available to future runs.`);
      }
      resetForm();
      await loadServers();
    } catch (reason) {
      setError(reason.message);
    } finally {
      setSaving(false);
    }
  }

  async function importServers(file) {
    if (!file) return;
    setError("");
    setNotice("");
    try {
      let document;
      try {
        document = JSON.parse(await file.text());
      } catch {
        throw new Error(`${file.name} is not valid JSON.`);
      }
      const result = await api("/api/servers", {
        method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(document),
      });
      resetForm();
      setServers(result.servers);
      setNotice(`Imported ${result.servers.length} servers from ${file.name}. They replace the previous list for future runs.`);
    } catch (reason) {
      setError(reason.message);
    } finally {
      if (importInputRef.current) importInputRef.current.value = "";
    }
  }

  async function removeServer(server) {
    if (!window.confirm(`Delete server ${server.id}? This affects future simulation runs.`)) return;
    setError("");
    setNotice("");
    try {
      await api(`/api/servers/${encodeURIComponent(server.id)}`, { method: "DELETE" });
      if (editingId === server.id) resetForm();
      setNotice(`Server ${server.id} deleted. Existing traces are unchanged.`);
      await loadServers();
    } catch (reason) {
      setError(reason.message);
    }
  }

  async function runSimulation(event) {
    event.preventDefault();
    setError("");
    setNotice("");
    setRunning(true);
    const form = new FormData();
    if (selectedFile) form.append("requests_file", selectedFile);
    form.append("allow_concurrency", String(allowConcurrency));
    form.append("scheduling_strategy", strategy);
    form.append("enable_autoscaling", String(enableAutoscaling));
    for (const [key, value] of Object.entries(autoscaleSettings)) form.append(key, value);
    if (enableQueuePolicy) {
      for (const [key, value] of Object.entries(queuePolicy)) {
        if (key === "starvation_guard_ticks" && !allowConcurrency) continue;
        if (String(value).trim() !== "") form.append(key, String(value).trim());
      }
    }
    try {
      const result = await api("/api/simulations", { method: "POST", body: form });
      setLatest(result);
      setNotice("Simulation complete. Metrics and the trace are ready below.");
      await loadRunDetails(result.run_id);
    } catch (reason) {
      setError(reason.message);
    } finally {
      setRunning(false);
    }
  }

  const metrics = runDetails.metrics;
  const clusterSeries = metrics?.cluster_series || [];
  const serverSeries = metrics?.server_series || {};
  const cpuLines = [
    { name: "Cluster", key: "cpu_utilization_percent", points: clusterSeries, color: "#4568e8" },
    ...Object.entries(serverSeries).map(([serverId, points], index) => ({
      name: serverId, key: "cpu_utilization_percent", points, color: chartColors[(index + 1) % chartColors.length],
    })),
  ];
  const autoscaleEvents = metrics?.autoscaling?.decisions || [];
  const ticksPerPoint = metrics?.series_ticks_per_point || 1;
  const sampleLabel = metrics
    ? `${metrics.sample_count.toLocaleString()} ticks${ticksPerPoint > 1 ? ` · ${ticksPerPoint} per chart point` : ""}`
    : "…";
  const serversExport = `data:application/json;charset=utf-8,${encodeURIComponent(
    JSON.stringify({ tick_seconds: 1, servers }, null, 2),
  )}`;
  const dropReasons = Object.entries(latest?.summary?.drop_reasons || {}).filter(([, count]) => count > 0);
  const queuePolicyNote = Object.entries(latest?.summary?.queue_policy || {})
    .map(([key, value]) => `${key.replaceAll("_", " ")} ${value}`)
    .join(", ");

  return (
    <main className="page-shell">
      <header className="topbar">
        <div className="brand-mark" aria-hidden="true">LB</div>
        <div><p className="eyebrow">SERVER MANAGEMENT DASHBOARD</p><h1>Load balancer simulation</h1></div>
        <div className="tick-badge"><span className="pulse-dot" /> Discrete time · 1 second ticks</div>
      </header>

      {(error || notice) && (
        <div className={`message ${error ? "message-error" : "message-success"}`} role="status">
          <span>{error || notice}</span>
          <button className="message-close" onClick={() => { setError(""); setNotice(""); }} aria-label="Dismiss message">×</button>
        </div>
      )}

      <section className="intro-row">
        <div><h2>Cluster control</h2><p>Configure capacity and choose how requests are placed, measured, and scaled.</p></div>
        <div className="server-count"><strong>{servers.length}</strong><span>{servers.length === 1 ? "server" : "servers"} configured</span></div>
      </section>

      <section className="content-grid">
        <div className="panel server-panel">
          <div className="panel-heading">
            <div><p className="eyebrow">CAPACITY POOL</p><h3>Servers</h3></div>
            <span className="count-chip">{servers.length}</span>
          </div>
          <div className="server-file-actions">
            <label className="text-button file-button">
              Import servers.json
              <input ref={importInputRef} type="file" accept=".json,application/json" onChange={(event) => importServers(event.target.files?.[0])} />
            </label>
            <a className="text-button" href={serversExport} download="servers.json">Export servers.json</a>
          </div>
          {loading ? <div className="empty-state">Loading server configuration…</div> : servers.length === 0 ? (
            <div className="empty-state">No servers are configured. Add one below to run a simulation.</div>
          ) : (
            <div className="table-wrap">
              <table>
                <thead><tr><th>Server</th><th>CPU / tick</th><th>Memory</th><th>Starts / tick</th><th><span className="sr-only">Actions</span></th></tr></thead>
                <tbody>{servers.map((server) => (
                  <tr key={server.id}>
                    <td><span className="server-icon">▦</span><strong>{server.id}</strong></td>
                    <td>{server.cpu_units_per_tick} units</td><td>{server.mem_mb.toLocaleString()} MB</td><td>{server.rate_limit_per_sec}</td>
                    <td className="row-actions"><button className="text-button" onClick={() => startEdit(server)}>Edit</button><button className="text-button danger-text" onClick={() => removeServer(server)}>Delete</button></td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          )}

          <div className="form-divider" />
          <div className="form-heading"><h4>{editingId ? `Edit ${editingId}` : "Add a server"}</h4>{editingId && <button className="text-button" onClick={resetForm}>Cancel edit</button>}</div>
          <form className="server-form" onSubmit={saveServer}>
            <label>Server ID<input value={draft.id} onChange={(event) => updateDraft("id", event.target.value)} disabled={Boolean(editingId)} placeholder="e.g. s3" maxLength={80} pattern={SERVER_ID_PATTERN} title="Letters, digits, '.', '_' or '-', starting with a letter or digit" required /></label>
            <div className="form-fields">
              <label>CPU units per tick<input type="number" min="1" step="1" value={draft.cpu_units_per_tick} onChange={(event) => updateDraft("cpu_units_per_tick", event.target.value)} required /></label>
              <label>Memory (MB)<input type="number" min="1" step="1" value={draft.mem_mb} onChange={(event) => updateDraft("mem_mb", event.target.value)} required /></label>
              <label>Start limit per tick<input type="number" min="0" step="1" value={draft.rate_limit_per_sec} onChange={(event) => updateDraft("rate_limit_per_sec", event.target.value)} required /></label>
            </div>
            <button className="primary-button" type="submit" disabled={saving}>{saving ? "Saving…" : editingId ? "Save changes" : "Add server"}</button>
          </form>
        </div>

        <aside className="right-column">
          <section className="panel run-panel">
            <div className="panel-heading"><div><p className="eyebrow">SIMULATION</p><h3>Run workload</h3></div><span className="run-icon">▶</span></div>
            <p className="panel-copy">Use <code>requests.csv</code> or upload a compatible workload.</p>
            <form onSubmit={runSimulation}>
              <label className="upload-box">
                <span className="upload-glyph">↑</span>
                <span><strong>{selectedFile ? selectedFile.name : "Choose request CSV"}</strong><small>{selectedFile ? "Selected for this run" : "Leave empty to use the sample data"}</small></span>
                <input ref={fileInputRef} type="file" accept=".csv,text/csv" onChange={(event) => setSelectedFile(event.target.files?.[0] || null)} />
              </label>
              {selectedFile && <button type="button" className="text-button clear-file" onClick={() => { setSelectedFile(null); if (fileInputRef.current) fileInputRef.current.value = ""; }}>Clear uploaded file</button>}
              <label className="option-card">
                <input type="checkbox" checked={allowConcurrency} onChange={(event) => setAllowConcurrency(event.target.checked)} />
                <span><strong>Allow concurrent requests</strong><small>Requests share server CPU. The supplied validator assumes one active request per server.</small></span>
              </label>
              <label className="select-field">Scheduling strategy
                <select value={strategy} onChange={(event) => setStrategy(event.target.value)}>
                  {strategies.map((name) => <option key={name} value={name}>{strategyLabels[name] || name.replaceAll("_", " ")}</option>)}
                </select>
              </label>
              <label className="option-card autoscale-toggle">
                <input type="checkbox" checked={enableAutoscaling} onChange={(event) => setEnableAutoscaling(event.target.checked)} />
                <span><strong>Enable CPU autoscaling</strong><small>Adjust CPU capacity during the run using utilization, queue, and drop-rate signals.</small></span>
              </label>
              {enableAutoscaling && (
                <div className="autoscale-settings">
                  <label>Scale up CPU at (%)<input type="number" min="1" max="100" value={autoscaleSettings.scale_up_cpu_percent} onChange={(event) => updateAutoscale("scale_up_cpu_percent", event.target.value)} /></label>
                  <label>Scale down CPU below (%)<input type="number" min="0" max="99" value={autoscaleSettings.scale_down_cpu_percent} onChange={(event) => updateAutoscale("scale_down_cpu_percent", event.target.value)} /></label>
                  <label>Queue threshold<input type="number" min="1" value={autoscaleSettings.queue_threshold} onChange={(event) => updateAutoscale("queue_threshold", event.target.value)} /></label>
                  <label>Error rate threshold (%)<input type="number" min="1" max="100" value={autoscaleSettings.error_rate_threshold_percent} onChange={(event) => updateAutoscale("error_rate_threshold_percent", event.target.value)} /></label>
                  <label>Cooldown (ticks)<input type="number" min="1" value={autoscaleSettings.cooldown_ticks} onChange={(event) => updateAutoscale("cooldown_ticks", event.target.value)} /></label>
                  <label>Minimum CPU (% of baseline)<input type="number" min="1" max="100" value={autoscaleSettings.min_cpu_percent} onChange={(event) => updateAutoscale("min_cpu_percent", event.target.value)} /></label>
                  <label>Maximum CPU (% of baseline)<input type="number" min="100" max="1000" value={autoscaleSettings.max_cpu_percent} onChange={(event) => updateAutoscale("max_cpu_percent", event.target.value)} /></label>
                </div>
              )}
              <label className="option-card">
                <input type="checkbox" checked={enableQueuePolicy} onChange={(event) => setEnableQueuePolicy(event.target.checked)} />
                <span><strong>Limit queueing under overload</strong><small>Drop requests when the queue is full or they wait too long. Leave a field empty to keep that limit off.</small></span>
              </label>
              {enableQueuePolicy && (
                <div className="autoscale-settings">
                  <label>Max queue length<input type="number" min="0" placeholder="Unlimited" value={queuePolicy.max_queue_length} onChange={(event) => updateQueuePolicy("max_queue_length", event.target.value)} /></label>
                  <label>Max wait (ticks)<input type="number" min="0" placeholder="Unlimited" value={queuePolicy.max_wait_ticks} onChange={(event) => updateQueuePolicy("max_wait_ticks", event.target.value)} /></label>
                  {allowConcurrency && (
                    <label>Starvation guard (ticks)<input type="number" min="0" placeholder="Off" value={queuePolicy.starvation_guard_ticks} onChange={(event) => updateQueuePolicy("starvation_guard_ticks", event.target.value)} /></label>
                  )}
                </div>
              )}
              <button className="primary-button run-button" type="submit" disabled={running || loading || servers.length === 0}>
                <span>{running ? "Running simulation…" : "Run simulation"}</span><span aria-hidden="true">→</span>
              </button>
            </form>
            {servers.length === 0 && <p className="inline-hint">Add at least one server before starting a run.</p>}
          </section>

          <section className="panel result-panel">
            <div className="panel-heading result-heading">
              <div><p className="eyebrow">LATEST RESULT</p><h3>Run summary</h3></div>
              {latest && <span className="valid-chip"><span /> Complete</span>}
            </div>
            {!latest ? <div className="empty-state result-empty">Run a workload to see its summary and trace here.</div> : (
              <>
                <div className="summary-grid">
                  <div className="metric-card"><span>Requests</span><strong>{latest.summary.total_requests}</strong></div>
                  <div className="metric-card"><span>Finished</span><strong>{latest.summary.finished}</strong></div>
                  <div className="metric-card"><span>Dropped</span><strong>{latest.summary.dropped}</strong></div>
                  <div className="metric-card"><span>Average wait</span><strong className="metric-small">{formatWait(latest.summary.avg_wait_ticks)}</strong></div>
                  <div className="metric-card"><span>p95 wait</span><strong className="metric-small">{formatWait(latest.summary.p95_wait_ticks)}</strong></div>
                  <div className="metric-card"><span>Maximum wait</span><strong className="metric-small">{formatWait(latest.summary.max_wait_ticks)}</strong></div>
                </div>
                {dropReasons.length > 0 && (
                  <p className="run-mode-note">Dropped: {dropReasons.map(([reason, count]) => `${count} ${dropReasonLabels[reason] || reason}`).join(" · ")}</p>
                )}
                <p className="run-mode-note">{latest.summary.execution_mode === "shared_cpu" ? "Shared CPU" : "One request per server"} · {latest.summary.scheduling_strategy?.replaceAll("_", " ") || "projected drain"}{latest.summary.autoscaling_enabled ? " · autoscaling on" : ""}{queuePolicyNote ? ` · ${queuePolicyNote}` : ""}</p>
                <div className="run-id-row"><span>Deterministic run ID</span><code title={latest.run_id}>{latest.run_id.slice(0, 12)}…</code></div>
                <a className="download-link" href={`/api/simulations/${latest.run_id}/trace`} download="run.jsonl"><span>Download run.jsonl</span><span aria-hidden="true">↓</span></a>
              </>
            )}
          </section>
        </aside>
      </section>

      <section className="analysis-section">
        <div className="panel analysis-panel">
          <div className="panel-heading analysis-heading"><div><p className="eyebrow">RUN VISUALIZATION</p><h3>Request timeline</h3></div>{latest && <span className="count-chip">{sampleLabel}</span>}</div>
          {detailsLoading ? <div className="empty-state">Loading trace and run details…</div> : detailsError ? <div className="empty-state">Metrics are unavailable for this saved run. Start a new run to create the visualizations. ({detailsError})</div> : <RequestTimeline events={runDetails.events} metrics={metrics} />}
        </div>

        <div className="panel analysis-panel">
          <div className="panel-heading analysis-heading"><div><p className="eyebrow">PERFORMANCE</p><h3>Cluster and server metrics</h3></div>{metrics && <span className="count-chip">{sampleLabel}</span>}</div>
          {!metrics ? <div className="empty-state">Run a workload to see CPU, memory, queue, and error-rate metrics.</div> : (
            <>
              <div className="summary-grid performance-cards">
                <div className="metric-card"><span>Average cluster CPU</span><strong>{formatPercent(metrics.cluster.avg_cpu_utilization_percent)}</strong></div>
                <div className="metric-card"><span>Peak cluster CPU</span><strong>{formatPercent(metrics.cluster.peak_cpu_utilization_percent)}</strong></div>
                <div className="metric-card"><span>Peak memory</span><strong>{formatPercent(metrics.cluster.peak_memory_utilization_percent)}</strong></div>
                <div className="metric-card"><span>Average queue</span><strong>{metrics.cluster.avg_queue_length}</strong></div>
                <div className="metric-card"><span>Peak queue</span><strong>{metrics.cluster.peak_queue_length}</strong></div>
                <div className="metric-card"><span>Average drop rate</span><strong>{formatPercent(metrics.cluster.avg_error_rate_percent)}</strong></div>
                <div className="metric-card"><span>Peak drop rate</span><strong>{formatPercent(metrics.cluster.peak_error_rate_percent)}</strong></div>
                <div className="metric-card"><span>Peak active</span><strong>{metrics.cluster.peak_active_requests}</strong></div>
                <div className="metric-card"><span>Work completed</span><strong className="metric-small">{metrics.cluster.total_work_completed_units}</strong></div>
              </div>
              <div className="charts-grid">
                <SeriesChart title="CPU utilization" points={clusterSeries} lines={cpuLines} maxValue={100} />
                <SeriesChart title="Queue length" points={clusterSeries} lines={[{ name: "Queued requests", key: "queued_requests", points: clusterSeries, color: "#d78a35" }]} maxValue={Math.max(1, metrics.cluster.peak_queue_length)} suffix="" />
              </div>
              <div className="table-wrap performance-table-wrap">
                <table>
                  <thead><tr><th>Server</th><th>Avg CPU</th><th>Peak CPU</th><th>Avg memory</th><th>Peak memory</th><th>Peak active</th><th>Starts</th></tr></thead>
                  <tbody>{Object.entries(metrics.per_server).sort(([a], [b]) => a.localeCompare(b)).map(([serverId, item]) => (
                    <tr key={serverId}><td><strong>{serverId}</strong></td><td>{formatPercent(item.avg_cpu_utilization_percent)}</td><td>{formatPercent(item.peak_cpu_utilization_percent)}</td><td>{formatPercent(item.avg_memory_utilization_percent)}</td><td>{formatPercent(item.peak_memory_utilization_percent)}</td><td>{item.peak_active_requests}</td><td>{item.requests_started}</td></tr>
                  ))}</tbody>
                </table>
              </div>
              <p className="chart-note">CPU utilization is completed work divided by configured capacity; memory shows reserved request memory. Drop rate is dropped requests per arrival in each tick, capped at 100%; timeouts in a tick without arrivals count as 100%.{ticksPerPoint > 1 ? ` Long runs are charted as averages of ${ticksPerPoint} ticks per point; the summary values use every tick.` : ""}</p>
            </>
          )}
        </div>

        {metrics?.autoscaling?.enabled && (
          <div className="panel analysis-panel autoscale-results">
            <div className="panel-heading analysis-heading"><div><p className="eyebrow">AUTOSCALER</p><h3>Capacity decisions</h3></div><span className="count-chip">{autoscaleEvents.length} changes</span></div>
            {autoscaleEvents.length === 0 ? <p className="panel-copy">No capacity changes were needed under this policy.</p> : (
              <div className="table-wrap"><table><thead><tr><th>Tick</th><th>Server</th><th>Action</th><th>CPU before</th><th>CPU after</th><th>CPU / queue / errors</th></tr></thead>
                <tbody>{autoscaleEvents.map((decision, index) => <tr key={`${decision.tick}-${decision.server_id}-${index}`}><td>{decision.tick}</td><td><strong>{decision.server_id}</strong></td><td><span className={`scale-direction ${decision.direction}`}>{decision.direction === "up" ? "Scale up" : "Scale down"}</span></td><td>{decision.cpu_before}</td><td>{decision.cpu_after}</td><td>{formatPercent(decision.signals.cpu_utilization_percent)} / {decision.signals.queued_requests} / {formatPercent(decision.signals.error_rate_percent)}</td></tr>)}</tbody>
              </table></div>
            )}
            <p className="chart-note">CPU changes apply within this simulated run and do not alter the saved server configuration.</p>
          </div>
        )}
      </section>
      <footer>FIFO request queue · Selectable placement policy · Per-tick metrics · Optional CPU autoscaling and overload limits</footer>
    </main>
  );
}
