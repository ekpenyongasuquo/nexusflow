"use client";

import { useState, useEffect, useCallback } from "react";
import {
  Activity, AlertTriangle, CheckCircle, Clock,
  XCircle, ChevronRight, Zap, Shield, BarChart3,
  RefreshCw, Play, ThumbsUp, ThumbsDown, ArrowRight,
  FileText, Lock
} from "lucide-react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ── Types ─────────────────────────────────────────────────────────────────────
interface Pipeline {
  pipeline_id: string;
  status: string;
  trigger_type: string;
  created_at: string;
  duration_seconds: number | null;
  brief_summary: string | null;
  options_count: number;
  error_message: string | null;
}

interface PipelineDetail {
  pipeline_id: string;
  status: string;
  trigger_type: string;
  brief: {
    context_summary: string;
    causal_chain: string[];
    confidence_score: number;
    estimated_impact_usd: number | null;
    risk_matrix: { factor: string; likelihood: string; impact: string; mitigation: string }[];
  } | null;
  validation: {
    required_approver_role: string;
    pii_findings: { entity_type: string }[];
    policy_violations: { rule_id: string; severity: string; description: string }[];
    is_cleared: boolean;
  } | null;
  recommendation: {
    options: {
      option_id: string;
      label: string;
      title: string;
      description: string;
      confidence: number;
      risk_level: string;
      implementation_steps: string[];
      time_to_implement_days: number | null;
    }[];
    reasoning: string;
  } | null;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const STATUS_CONFIG: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  PENDING:          { color: "text-slate-400 bg-slate-800",  icon: <Clock size={12} />,         label: "Pending" },
  COLLECTING:       { color: "text-blue-400 bg-blue-900/40", icon: <Activity size={12} />,      label: "Collecting" },
  SYNTHESISING:     { color: "text-purple-400 bg-purple-900/40", icon: <Zap size={12} />,       label: "Synthesising" },
  VALIDATING:       { color: "text-yellow-400 bg-yellow-900/40", icon: <Shield size={12} />,    label: "Validating" },
  RECOMMENDING:     { color: "text-indigo-400 bg-indigo-900/40", icon: <BarChart3 size={12} />, label: "Recommending" },
  AWAITING_HUMAN:   { color: "text-cyan-400 bg-cyan-900/40",  icon: <AlertTriangle size={12} />, label: "Awaiting Approval" },
  EXECUTING:        { color: "text-orange-400 bg-orange-900/40", icon: <Play size={12} />,      label: "Executing" },
  COMPLETE:         { color: "text-green-400 bg-green-900/40", icon: <CheckCircle size={12} />, label: "Complete" },
  FAILED:           { color: "text-red-400 bg-red-900/40",    icon: <XCircle size={12} />,      label: "Failed" },
  HALTED_COMPLIANCE:{ color: "text-amber-400 bg-amber-900/40", icon: <Lock size={12} />,        label: "Compliance Hold" },
};

const RISK_COLOR: Record<string, string> = {
  LOW: "text-green-400", MEDIUM: "text-yellow-400",
  HIGH: "text-orange-400", CRITICAL: "text-red-400",
};

function StatusBadge({ status }: { status: string }) {
  const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.PENDING;
  return (
    <span className={`badge gap-1 ${cfg.color}`}>
      {cfg.icon} {cfg.label}
    </span>
  );
}

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(value * 100);
  const color = pct >= 70 ? "bg-green-500" : pct >= 50 ? "bg-yellow-500" : "bg-red-500";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-slate-400 w-8 text-right">{pct}%</span>
    </div>
  );
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [token, setToken] = useState<string>("");
  const [loginForm, setLoginForm] = useState({ email: "", password: "" });
  const [loginError, setLoginError] = useState("");
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [selected, setSelected] = useState<PipelineDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [triggerType, setTriggerType] = useState("BUDGET_VARIANCE");
  const [triggerMeta, setTriggerMeta] = useState('{"slack_channel_id": "", "jira_project_key": ""}');
  const [approving, setApproving] = useState(false);
  const [selectedOptionId, setSelectedOptionId] = useState<string>("");

  // ── Auth ──────────────────────────────────────────────────────────────────
  const handleLogin = async () => {
    setLoginError("");
    try {
      const resp = await fetch(`${API}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(loginForm),
      });
      if (!resp.ok) { setLoginError("Invalid credentials"); return; }
      const data = await resp.json();
      setToken(data.access_token);
    } catch {
      setLoginError("Connection failed — is the API running?");
    }
  };

  const authHeaders = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };

  // ── Pipeline list ──────────────────────────────────────────────────────────
  const fetchPipelines = useCallback(async () => {
    if (!token) return;
    const resp = await fetch(`${API}/pipelines/`, { headers: authHeaders });
    if (resp.ok) setPipelines(await resp.json());
  }, [token]);

  useEffect(() => {
    fetchPipelines();
    const interval = setInterval(fetchPipelines, 5000); // poll every 5s
    return () => clearInterval(interval);
  }, [fetchPipelines]);

  // ── Pipeline detail ───────────────────────────────────────────────────────
  const openDetail = async (id: string) => {
    const resp = await fetch(`${API}/pipelines/${id}/detail`, { headers: authHeaders });
    if (resp.ok) {
      const detail = await resp.json();
      setSelected(detail);
      if (detail.recommendation?.options?.[0]) {
        setSelectedOptionId(detail.recommendation.options[0].option_id);
      }
    }
  };

  // ── Trigger ───────────────────────────────────────────────────────────────
  const triggerPipeline = async () => {
    setLoading(true);
    try {
      let meta = {};
      try { meta = JSON.parse(triggerMeta); } catch { meta = {}; }
      await fetch(`${API}/pipelines/trigger`, {
        method: "POST",
        headers: authHeaders,
        body: JSON.stringify({ trigger_type: triggerType, trigger_metadata: meta }),
      });
      await fetchPipelines();
    } finally { setLoading(false); }
  };

  // ── Approve ───────────────────────────────────────────────────────────────
  const submitApproval = async (outcome: string) => {
    if (!selected) return;
    setApproving(true);
    try {
      await fetch(`${API}/pipelines/${selected.pipeline_id}/approve`, {
        method: "POST",
        headers: authHeaders,
        body: JSON.stringify({
          outcome,
          selected_option_id: outcome === "APPROVED" ? selectedOptionId : null,
        }),
      });
      setSelected(null);
      await fetchPipelines();
    } finally { setApproving(false); }
  };

  // ── Login screen ──────────────────────────────────────────────────────────
  if (!token) {
    return (
      <div className="min-h-screen flex items-center justify-center p-4">
        <div className="w-full max-w-md">
          <div className="text-center mb-8">
            <div className="inline-flex items-center gap-2 mb-4">
              <div className="w-10 h-10 rounded-lg bg-cyan-500 flex items-center justify-center">
                <Zap size={20} className="text-slate-950" />
              </div>
              <span className="text-2xl font-bold text-white">NexusFlow</span>
            </div>
            <p className="text-slate-400 text-sm">Autonomous Decision Intelligence</p>
          </div>
          <div className="card space-y-4">
            <h2 className="text-lg font-semibold">Sign In</h2>
            {loginError && (
              <div className="bg-red-900/30 border border-red-800 rounded-lg p-3 text-red-400 text-sm">
                {loginError}
              </div>
            )}
            <input
              type="email" placeholder="Email"
              className="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-cyan-500"
              value={loginForm.email}
              onChange={e => setLoginForm(f => ({ ...f, email: e.target.value }))}
              onKeyDown={e => e.key === "Enter" && handleLogin()}
            />
            <input
              type="password" placeholder="Password"
              className="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-cyan-500"
              value={loginForm.password}
              onChange={e => setLoginForm(f => ({ ...f, password: e.target.value }))}
              onKeyDown={e => e.key === "Enter" && handleLogin()}
            />
            <button onClick={handleLogin} className="btn-primary w-full">Sign In</button>
            <p className="text-slate-500 text-xs text-center">
              No account? POST to /auth/register to create one.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // ── Main Dashboard ────────────────────────────────────────────────────────
  const awaitingApproval = pipelines.filter(p => p.status === "AWAITING_HUMAN");

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 h-14 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded bg-cyan-500 flex items-center justify-center">
              <Zap size={14} className="text-slate-950" />
            </div>
            <span className="font-bold text-white">NexusFlow</span>
            <span className="text-slate-600 text-sm hidden sm:block">/ Dashboard</span>
          </div>
          <div className="flex items-center gap-3">
            {awaitingApproval.length > 0 && (
              <span className="badge bg-cyan-900/60 text-cyan-400 gap-1">
                <AlertTriangle size={11} /> {awaitingApproval.length} pending
              </span>
            )}
            <button onClick={fetchPipelines} className="text-slate-400 hover:text-white transition-colors p-1">
              <RefreshCw size={15} />
            </button>
            <button onClick={() => setToken("")} className="text-slate-500 hover:text-slate-300 text-xs transition-colors">
              Sign out
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-6 space-y-6">

        {/* Stats row */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: "Total", value: pipelines.length, color: "text-white" },
            { label: "Awaiting", value: awaitingApproval.length, color: "text-cyan-400" },
            { label: "Complete", value: pipelines.filter(p => p.status === "COMPLETE").length, color: "text-green-400" },
            { label: "Failed", value: pipelines.filter(p => p.status === "FAILED").length, color: "text-red-400" },
          ].map(stat => (
            <div key={stat.label} className="card py-4">
              <div className={`text-2xl font-bold ${stat.color}`}>{stat.value}</div>
              <div className="text-slate-500 text-xs mt-0.5">{stat.label}</div>
            </div>
          ))}
        </div>

        {/* Trigger panel */}
        <div className="card">
          <h2 className="font-semibold text-white mb-4 flex items-center gap-2">
            <Play size={15} className="text-cyan-400" /> Trigger New Pipeline
          </h2>
          <div className="grid sm:grid-cols-3 gap-3">
            <div>
              <label className="text-xs text-slate-400 mb-1.5 block">Trigger Type</label>
              <select
                value={triggerType}
                onChange={e => setTriggerType(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-cyan-500"
              >
                {["BUDGET_VARIANCE","PROJECT_STALL","CUSTOMER_ESCALATION","COMPLIANCE_DEADLINE","ANOMALY_DETECTED","MANUAL"]
                  .map(t => <option key={t} value={t}>{t.replace("_"," ")}</option>)}
              </select>
            </div>
            <div className="sm:col-span-2">
              <label className="text-xs text-slate-400 mb-1.5 block">Trigger Metadata (JSON)</label>
              <input
                value={triggerMeta}
                onChange={e => setTriggerMeta(e.target.value)}
                className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:border-cyan-500"
                placeholder='{"slack_channel_id": "C001", "jira_labels": ["Q3-budget"]}'
              />
            </div>
          </div>
          <button
            onClick={triggerPipeline}
            disabled={loading}
            className="btn-primary mt-4 flex items-center gap-2 disabled:opacity-50"
          >
            {loading ? <RefreshCw size={14} className="animate-spin" /> : <Zap size={14} />}
            Trigger Pipeline
          </button>
        </div>

        {/* Pipeline list */}
        <div className="card">
          <h2 className="font-semibold text-white mb-4 flex items-center gap-2">
            <Activity size={15} className="text-cyan-400" /> Recent Pipelines
          </h2>
          {pipelines.length === 0 ? (
            <p className="text-slate-500 text-sm text-center py-8">
              No pipelines yet. Trigger one above.
            </p>
          ) : (
            <div className="divide-y divide-slate-800">
              {pipelines.map(p => (
                <div
                  key={p.pipeline_id}
                  className="py-3 flex items-center gap-3 hover:bg-slate-800/30 -mx-2 px-2 rounded-lg cursor-pointer transition-colors"
                  onClick={() => openDetail(p.pipeline_id)}
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="text-xs font-mono text-slate-400">{p.pipeline_id.slice(0, 8)}</span>
                      <StatusBadge status={p.status} />
                      {p.status === "AWAITING_HUMAN" && (
                        <span className="badge bg-cyan-900/60 text-cyan-300 text-[10px] animate-pulse">
                          ACTION REQUIRED
                        </span>
                      )}
                    </div>
                    <p className="text-sm text-slate-300 truncate">
                      {p.trigger_type.replace(/_/g, " ")}
                      {p.brief_summary && ` — ${p.brief_summary.slice(0, 80)}...`}
                    </p>
                    <p className="text-xs text-slate-500 mt-0.5">
                      {new Date(p.created_at).toLocaleString()}
                      {p.duration_seconds && ` · ${p.duration_seconds.toFixed(1)}s`}
                    </p>
                  </div>
                  <ChevronRight size={14} className="text-slate-600 shrink-0" />
                </div>
              ))}
            </div>
          )}
        </div>
      </main>

      {/* Pipeline detail modal */}
      {selected && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-start justify-center p-4 overflow-y-auto">
          <div className="w-full max-w-2xl my-8 space-y-4">

            {/* Header */}
            <div className="card">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-mono text-slate-400">{selected.pipeline_id.slice(0, 8)}</span>
                    <StatusBadge status={selected.status} />
                  </div>
                  <h2 className="font-bold text-white text-lg">
                    {selected.trigger_type.replace(/_/g, " ")}
                  </h2>
                </div>
                <button onClick={() => setSelected(null)} className="text-slate-400 hover:text-white text-xl leading-none">×</button>
              </div>
            </div>

            {/* Decision Brief */}
            {selected.brief && (
              <div className="card space-y-3">
                <h3 className="font-semibold text-cyan-400 flex items-center gap-2 text-sm">
                  <FileText size={13} /> Decision Brief
                </h3>
                <p className="text-sm text-slate-300 leading-relaxed">{selected.brief.context_summary}</p>

                {selected.brief.causal_chain.length > 0 && (
                  <div>
                    <p className="text-xs text-slate-500 mb-2 uppercase tracking-wide">Causal Chain</p>
                    <div className="flex flex-wrap gap-1">
                      {selected.brief.causal_chain.map((step, i) => (
                        <span key={i} className="flex items-center gap-1">
                          <span className="text-xs bg-slate-800 text-slate-300 px-2 py-0.5 rounded">{step}</span>
                          {i < selected.brief!.causal_chain.length - 1 && <ArrowRight size={10} className="text-slate-600" />}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                <div className="flex items-center gap-4">
                  <div>
                    <p className="text-xs text-slate-500">Confidence</p>
                    <ConfidenceBar value={selected.brief.confidence_score} />
                  </div>
                  {selected.brief.estimated_impact_usd && (
                    <div>
                      <p className="text-xs text-slate-500">Est. Impact</p>
                      <p className="text-sm font-semibold text-white">
                        ${selected.brief.estimated_impact_usd.toLocaleString()}
                      </p>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Validation */}
            {selected.validation && (
              <div className="card space-y-2">
                <h3 className="font-semibold text-yellow-400 flex items-center gap-2 text-sm">
                  <Shield size={13} /> Governance
                </h3>
                <div className="flex flex-wrap gap-2 text-xs">
                  <span className="badge bg-slate-800 text-slate-300">
                    Required Approver: <strong className="text-white ml-1">{selected.validation.required_approver_role}</strong>
                  </span>
                  <span className={`badge ${selected.validation.is_cleared ? "bg-green-900/40 text-green-400" : "bg-red-900/40 text-red-400"}`}>
                    {selected.validation.is_cleared ? "✓ Cleared" : "✗ Compliance Hold"}
                  </span>
                  {selected.validation.pii_findings.length > 0 && (
                    <span className="badge bg-amber-900/40 text-amber-400">
                      {selected.validation.pii_findings.length} PII pseudonymised
                    </span>
                  )}
                </div>
              </div>
            )}

            {/* Decision Options */}
            {selected.recommendation && selected.recommendation.options.length > 0 && (
              <div className="card space-y-3">
                <h3 className="font-semibold text-indigo-400 flex items-center gap-2 text-sm">
                  <BarChart3 size={13} /> Decision Options
                </h3>
                {selected.recommendation.options.map(opt => (
                  <div
                    key={opt.option_id}
                    className={`border rounded-lg p-3 cursor-pointer transition-colors ${
                      selectedOptionId === opt.option_id
                        ? "border-cyan-500 bg-cyan-900/20"
                        : "border-slate-700 hover:border-slate-600"
                    }`}
                    onClick={() => setSelectedOptionId(opt.option_id)}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-bold text-cyan-400">{opt.label}</span>
                        <span className="text-sm font-semibold text-white">{opt.title}</span>
                      </div>
                      <span className={`text-xs font-semibold ${RISK_COLOR[opt.risk_level] || "text-slate-400"}`}>
                        {opt.risk_level}
                      </span>
                    </div>
                    <p className="text-xs text-slate-400 mb-2">{opt.description}</p>
                    <ConfidenceBar value={opt.confidence} />
                    {opt.time_to_implement_days && (
                      <p className="text-xs text-slate-500 mt-1">
                        ⏱ {opt.time_to_implement_days} day{opt.time_to_implement_days !== 1 ? "s" : ""} to implement
                      </p>
                    )}
                  </div>
                ))}
              </div>
            )}

            {/* Approval actions */}
            {selected.status === "AWAITING_HUMAN" && (
              <div className="card">
                <h3 className="font-semibold text-white mb-3 text-sm">Your Decision</h3>
                <div className="grid grid-cols-2 gap-3">
                  <button
                    onClick={() => submitApproval("APPROVED")}
                    disabled={approving || !selectedOptionId}
                    className="btn-primary flex items-center justify-center gap-2 disabled:opacity-50"
                  >
                    <ThumbsUp size={14} /> Approve
                  </button>
                  <button
                    onClick={() => submitApproval("REJECTED")}
                    disabled={approving}
                    className="btn-danger flex items-center justify-center gap-2 disabled:opacity-50"
                  >
                    <ThumbsDown size={14} /> Reject
                  </button>
                  <button
                    onClick={() => submitApproval("ESCALATED")}
                    disabled={approving}
                    className="btn-ghost flex items-center justify-center gap-2 col-span-1 disabled:opacity-50"
                  >
                    Escalate
                  </button>
                  <button
                    onClick={() => submitApproval("DEFERRED")}
                    disabled={approving}
                    className="btn-ghost flex items-center justify-center gap-2 col-span-1 disabled:opacity-50"
                  >
                    Defer
                  </button>
                </div>
                {approving && (
                  <p className="text-xs text-slate-400 text-center mt-3 flex items-center justify-center gap-1">
                    <RefreshCw size={11} className="animate-spin" /> Executing decision...
                  </p>
                )}
              </div>
            )}

          </div>
        </div>
      )}
    </div>
  );
}
