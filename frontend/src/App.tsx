import React, { useState, useEffect, useRef, useCallback } from 'react';
import axios from 'axios';
import { PowerBIEmbed } from 'powerbi-client-react';
import { models } from 'powerbi-client';
import './App.css';

/** Renders a string that may contain https:// URLs as mixed text + <a> elements. */
function TextWithLinks({ text }: { text: string }) {
  const URL_RE = /(https?:\/\/[^\s)]+)/g;
  const parts = text.split(URL_RE);
  return (
    <>
      {parts.map((part, i) =>
        URL_RE.test(part) ? (
          <a key={i} href={part} target="_blank" rel="noreferrer" className="inline-link">
            {part}
          </a>
        ) : (
          <span key={i}>{part}</span>
        ),
      )}
    </>
  );
}

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp?: string;
  isError?: boolean;
  failedMessage?: string;
  /** Detailed run info when using the Assistants API */
  runDetails?: RunDetails | null;
}

interface RunDetails {
  run_status: string;
  sql_queries?: string[];
  sql_data_previews?: (string[] | null)[];
  data_retrieval_query?: string;
  answer?: string;
}

interface CompareResult {
  question: string;
  draft: { answer: string; run_status: string; sql_queries: string[]; error: string | null };
  production: { answer: string; run_status: string; sql_queries: string[]; error: string | null };
  match: boolean;
  timestamp: number;
}

interface AgentStatus {
  ready: boolean;
  message: string;
  agent_name: string | null;
  troubleshooting: string[];
}

interface PbiEmbedState {
  loading: boolean;
  config: models.IReportEmbedConfiguration | null;
  error: string;
  /** Fallback plain URL — used when embed token generation fails */
  fallbackUrl: string;
}

type ChatMode = 'detailed' | 'compare';

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || '';
const CHAT_TIMEOUT_MS = 120_000; // 2 minutes — matches backend Fabric query timeout

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const [agentStatus, setAgentStatus] = useState<AgentStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);

  // Chat mode: detailed (Assistants API with run details),
  // or compare (draft vs production side-by-side).
  const [chatMode, setChatMode] = useState<ChatMode>('detailed');

  // Thread name for Assistants API persistent conversations
  const [threadName, setThreadName] = useState('');

  // Compare mode state
  const [draftAgentId, setDraftAgentId] = useState('');
  const [prodAgentId, setProdAgentId] = useState('');
  const [compareResult, setCompareResult] = useState<CompareResult | null>(null);

  // Power BI embed state — driven by a backend-generated embed token so users
  // don't need to be signed into Power BI in their browser.
  const [pbi, setPbi] = useState<PbiEmbedState>({
    loading: true,
    config: null,
    error: '',
    fallbackUrl: '',
  });

  // Auto-publish state — tracks an in-flight /api/publish-agent call
  const [publishState, setPublishState] = useState<{
    loading: boolean;
    result: string;
    portalUrl: string;
  }>({ loading: false, result: '', portalUrl: '' });

  // ── Agent status check ──────────────────────────────────────────────────────
  const checkAgentStatus = useCallback(async () => {
    if (!BACKEND_URL) {
      setStatusLoading(false);
      return;
    }
    try {
      const resp = await axios.get(`${BACKEND_URL}/api/status`);
      setAgentStatus(resp.data);
    } catch {
      setAgentStatus({
        ready: false,
        message: 'Cannot reach the backend server. Please check that the backend is running.',
        agent_name: null,
        troubleshooting: [],
      });
    } finally {
      setStatusLoading(false);
    }
  }, []);

  // ── Power BI embed token fetch ──────────────────────────────────────────────
  // Fetches a short-lived embed token from the backend using the App Service's
  // Managed Identity, then renders the report via the Power BI JavaScript SDK.
  // This avoids the "app.powerbi.com refused to connect" error that occurs when
  // the browser blocks login-page redirects inside a plain <iframe>.
  const fetchPbiToken = useCallback(async () => {
    if (!BACKEND_URL) {
      setPbi({ loading: false, config: null, error: 'Backend URL not configured.', fallbackUrl: '' });
      return;
    }
    setPbi(prev => ({ ...prev, loading: true, error: '' }));
    try {
      const resp = await axios.get(`${BACKEND_URL}/api/powerbi-token`, { timeout: 20_000 });
      // Response shape mirrors the MS PowerBI-Developer-Samples EmbedConfig model:
      // { tokenId, accessToken, tokenExpiry, reportConfig: [{ reportId, reportName, embedUrl }] }
      const { accessToken, reportConfig } = resp.data;
      const report = reportConfig?.[0];

      setPbi({
        loading: false,
        error: '',
        fallbackUrl: '',
        config: {
          type: 'report',
          id: report?.reportId,
          embedUrl: report?.embedUrl,
          accessToken: accessToken,
          tokenType: models.TokenType.Embed,
          settings: {
            panes: {
              filters: { expanded: false, visible: false },
              pageNavigation: { visible: false },
            },
            background: models.BackgroundType.Transparent,
          },
        },
      });
    } catch (err: any) {
      const detail = err.response?.data?.detail;
      const msg =
        typeof detail === 'string'
          ? detail
          : 'Could not load the Power BI dashboard. ' +
            (err.message || 'Check backend logs for details.');

      // Fetch the raw embed URL from /api/config to use as a plain-iframe fallback.
      // This lets users who are already signed into Power BI in their browser
      // still see the report even when embed token generation is unavailable
      // (e.g. "Allow service principals to use Power BI APIs" not yet enabled).
      let fallbackUrl = '';
      try {
        const cfgResp = await axios.get(`${BACKEND_URL}/api/config`, { timeout: 8_000 });
        fallbackUrl = cfgResp.data?.powerbi_report_url || '';
      } catch {
        // silently ignore — fallback URL is optional
      }

      setPbi({ loading: false, config: null, error: msg, fallbackUrl });
    }
  }, []);

  // ── Try auto-publish the Fabric Data Agent ─────────────────────────────────
  // Calls the backend /api/publish-agent endpoint which uses the App Service
  // Managed Identity to attempt the Fabric publish API.  If the API is not yet
  // available on the tenant it returns a direct portal URL so the user can
  // publish in one click.
  const tryPublishAgent = useCallback(async () => {
    setPublishState({ loading: true, result: '', portalUrl: '' });
    try {
      const resp = await axios.post(`${BACKEND_URL}/api/publish-agent`, {}, { timeout: 70_000 });
      const { published, message, portal_url } = resp.data;
      setPublishState({ loading: false, result: message, portalUrl: portal_url || '' });
      if (published) {
        // Re-check status so the banner clears if the agent is now ready
        setTimeout(checkAgentStatus, 2000);
      }
    } catch (err: any) {
      const detail = err.response?.data?.detail;
      setPublishState({
        loading: false,
        result: typeof detail === 'string' ? detail : 'Auto-publish request failed. ' + (err.message || ''),
        portalUrl: '',
      });
    }
  }, [checkAgentStatus]);

  // ── Startup effects ─────────────────────────────────────────────────────────
  useEffect(() => {
    checkAgentStatus();
    fetchPbiToken();
  }, [checkAgentStatus, fetchPbiToken]);

  // Auto-refresh the embed token ~5 minutes before it expires (tokens last 1h).
  useEffect(() => {
    if (!pbi.config?.accessToken) return;
    const REFRESH_MS = 55 * 60 * 1000; // 55 minutes
    const timer = setTimeout(fetchPbiToken, REFRESH_MS);
    return () => clearTimeout(timer);
  }, [pbi.config?.accessToken, fetchPbiToken]);

  // ── Sample questions ────────────────────────────────────────────────────────
  const sampleQuestions = [
    'Top 5 customers by LifetimeValue in Maharashtra',
    'Which customers have ChurnRiskScore above 80?',
    'Show average MonthlyRevenue by State for Karnataka and Tamil Nadu',
    'Count customers by Segment',
    'List Startup customers in Delhi with LifetimeValue above 50000',
  ];

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  // ── Conversation reset ──────────────────────────────────────────────────────
  const resetConversation = async () => {
    try {
      await axios.post(`${BACKEND_URL}/api/reset`, { userId: 'web-user' });
    } catch (err) {
      console.warn('Reset request failed (clearing local state anyway):', err);
    }
    setMessages([]);
    setInput('');
    setCompareResult(null);
  };

  // ── Chat ────────────────────────────────────────────────────────────────────
  const sendMessage = async (overrideMessage?: string) => {
    const messageText = overrideMessage || input;
    if (!messageText.trim()) return;

    // In compare mode, delegate to the compare handler
    if (chatMode === 'compare') {
      return sendCompare(messageText);
    }

    const userMessage: Message = {
      role: 'user',
      content: messageText,
      timestamp: new Date().toISOString(),
    };

    setMessages(prev => [...prev, userMessage]);
    if (!overrideMessage) setInput('');
    setLoading(true);

    // AbortController enforces the same 2-minute timeout as the backend query.
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), CHAT_TIMEOUT_MS);

    try {
      let assistantMessage: Message;

      if (chatMode === 'detailed') {
        // Use the Assistants API for richer response (run details, SQL, etc.)
        const response = await axios.post(
          `${BACKEND_URL}/api/agent/run-details`,
          {
            question: messageText,
            thread_name: threadName || null,
          },
          { signal: controller.signal },
        );

        const details: RunDetails = {
          run_status: response.data.run_status,
          sql_queries: response.data.sql_queries || [],
          sql_data_previews: response.data.sql_data_previews || [],
          data_retrieval_query: response.data.data_retrieval_query || '',
          answer: response.data.answer || '',
        };

        assistantMessage = {
          role: 'assistant',
          content: response.data.answer || 'No response received.',
          timestamp: new Date(response.data.timestamp * 1000).toISOString(),
          runDetails: details,
        };
      } else {
        // Simple mode — existing REST API
        const response = await axios.post(
          `${BACKEND_URL}/api/chat`,
          { message: messageText, userId: 'web-user' },
          { signal: controller.signal },
        );
        assistantMessage = {
          role: 'assistant',
          content: response.data.answer || 'No response received.',
          timestamp: response.data.timestamp,
        };
      }

      setMessages(prev => [...prev, assistantMessage]);

      // If we got a successful response the agent is working — refresh banner.
      if (agentStatus && !agentStatus.ready) checkAgentStatus();
    } catch (error: any) {
      let errorText: string;

      if (error.name === 'AbortError' || error.code === 'ERR_CANCELED') {
        errorText =
          '⏱️ Request timed out after 2 minutes. The Fabric Data Agent may be under heavy load. Please try again.';
      } else {
        const detail = error.response?.data?.detail;
        if (detail && typeof detail === 'object' && detail.error === 'agent_not_ready') {
          const steps = (detail.troubleshooting || []) as string[];
          errorText =
            '⚠️ The AI Agent is not ready yet.\n\n' +
            (steps.length > 0
              ? 'To fix this:\n' + steps.map((s: string, i: number) => `${i + 1}. ${s}`).join('\n')
              : detail.message || 'Please check the backend /api/debug endpoint for details.');

          setAgentStatus(prev => ({
            ready: false,
            message:
              typeof detail.message === 'string'
                ? detail.message
                : (prev?.message || 'The AI Agent is not ready yet. Please publish it in the Fabric portal.'),
            agent_name: prev?.agent_name ?? null,
            troubleshooting: steps.length > 0 ? steps : (prev?.troubleshooting ?? []),
          }));
        } else {
          errorText = `Error: ${
            typeof detail === 'string' ? detail : error.message || 'Failed to get response'
          }`;
        }
      }

      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: errorText,
          timestamp: new Date().toISOString(),
          isError: true,
          failedMessage: messageText,
        },
      ]);
    } finally {
      clearTimeout(timeoutId);
      setLoading(false);
    }
  };

  // ── Compare: draft vs production ────────────────────────────────────────────
  const sendCompare = async (messageText: string) => {
    if (!draftAgentId.trim() || !prodAgentId.trim()) {
      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: '⚠️ Please enter both Draft and Production Agent IDs before comparing.',
          timestamp: new Date().toISOString(),
          isError: true,
        },
      ]);
      return;
    }

    const userMessage: Message = {
      role: 'user',
      content: `[Compare] ${messageText}`,
      timestamp: new Date().toISOString(),
    };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      const response = await axios.post(
        `${BACKEND_URL}/api/agent/compare`,
        {
          question: messageText,
          draft_agent_id: draftAgentId,
          production_agent_id: prodAgentId,
        },
        { timeout: CHAT_TIMEOUT_MS },
      );
      setCompareResult(response.data);

      const matchIcon = response.data.match ? '✅' : '❌';
      const summary =
        `${matchIcon} Responses ${response.data.match ? 'MATCH' : 'DIFFER'}\n\n` +
        `**Draft answer:**\n${response.data.draft?.answer || response.data.draft?.error || '(no response)'}\n\n` +
        `**Production answer:**\n${response.data.production?.answer || response.data.production?.error || '(no response)'}`;

      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: summary,
          timestamp: new Date().toISOString(),
        },
      ]);
    } catch (error: any) {
      const detail = error.response?.data?.detail;
      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: `Error comparing agents: ${typeof detail === 'string' ? detail : error.message}`,
          timestamp: new Date().toISOString(),
          isError: true,
          failedMessage: messageText,
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const retryMessage = (message: string) => sendMessage(message);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const agentNotReady = !statusLoading && agentStatus && !agentStatus.ready;

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div className="app-container">
      {/* ── Left panel: Chat ── */}
      <div className="chat-panel">
        <div className="chat-header">
          <div className="chat-header-content">
            <div>
              <h1>🤖 Customer 360 AI Analytics</h1>
              <p>Ask questions about your customer data</p>
            </div>
            {messages.length > 0 && (
              <button className="new-chat-btn" onClick={resetConversation}>
                🔄 New Chat
              </button>
            )}
          </div>

          {/* ── Mode toggle ── */}
          <div className="mode-toggle">
            {(['detailed', 'compare'] as ChatMode[]).map(mode => (
              <button
                key={mode}
                className={`mode-btn${chatMode === mode ? ' mode-btn--active' : ''}`}
                onClick={() => setChatMode(mode)}
              >
                {mode === 'detailed' && '🔍 Detailed'}
                {mode === 'compare' && '⚖️ Compare'}
              </button>
            ))}
          </div>

          {/* ── Detailed mode: thread name ── */}
          {chatMode === 'detailed' && (
            <div className="thread-input-row">
              <label htmlFor="thread-name">🧵 Thread:</label>
              <input
                id="thread-name"
                type="text"
                value={threadName}
                onChange={e => setThreadName(e.target.value)}
                placeholder="(optional) persistent thread name"
                className="thread-input"
              />
            </div>
          )}

          {/* ── Compare mode: agent IDs ── */}
          {chatMode === 'compare' && (
            <div className="compare-config">
              <div className="compare-config-row">
                <label>Draft Agent ID:</label>
                <input
                  type="text"
                  value={draftAgentId}
                  onChange={e => setDraftAgentId(e.target.value)}
                  placeholder="Fabric Data Agent GUID (draft)"
                  className="thread-input"
                />
              </div>
              <div className="compare-config-row">
                <label>Production Agent ID:</label>
                <input
                  type="text"
                  value={prodAgentId}
                  onChange={e => setProdAgentId(e.target.value)}
                  placeholder="Fabric Data Agent GUID (production)"
                  className="thread-input"
                />
              </div>
            </div>
          )}
        </div>



        <div className="messages-container">
          {messages.length === 0 && (
            <div className="welcome-message">
              <h2>Welcome! 👋</h2>
              <p>Ask me anything about your customer data. Try these questions:</p>
              <div className="sample-questions">
                {sampleQuestions.map((q, idx) => (
                  <button
                    key={idx}
                    className={`sample-question-btn${agentNotReady ? ' sample-question-btn--disabled' : ''}`}
                    onClick={() => !agentNotReady && setInput(q)}
                    disabled={!!agentNotReady}
                    title={agentNotReady ? 'AI Agent is not ready' : ''}
                  >
                    {q}
                  </button>
                ))}
              </div>
              {agentNotReady && (
                <p className="small-text" style={{ color: '#e67e22', marginTop: '8px' }}>
                  Sample questions are disabled until the AI Agent is ready.
                </p>
              )}
            </div>
          )}

          {messages.map((msg, idx) => (
            <div
              key={idx}
              className={`message message-${msg.role}${msg.isError ? ' message-error' : ''}`}
            >
              <div className="message-avatar">
                {msg.role === 'user' ? '👤' : msg.isError ? '⚠️' : '🤖'}
              </div>
              <div className="message-content">
                <div className="message-text" style={{ whiteSpace: 'pre-wrap' }}>
                  {msg.isError
                    ? msg.content.split('\n').map((line, li) => (
                        <span key={li}>
                          <TextWithLinks text={line} />
                          {'\n'}
                        </span>
                      ))
                    : msg.content}
                </div>

                {/* Run details panel (detailed mode) */}
                {msg.runDetails && (
                  <details className="run-details-panel">
                    <summary>
                      🔍 Run Details — Status: {msg.runDetails.run_status}
                      {msg.runDetails.sql_queries && msg.runDetails.sql_queries.length > 0
                        ? ` · ${msg.runDetails.sql_queries.length} SQL quer${msg.runDetails.sql_queries.length === 1 ? 'y' : 'ies'}`
                        : ''}
                    </summary>

                    {msg.runDetails.data_retrieval_query && (
                      <div className="run-detail-section">
                        <strong>🎯 Data Retrieval Query:</strong>
                        <pre className="sql-block">{msg.runDetails.data_retrieval_query}</pre>
                      </div>
                    )}

                    {msg.runDetails.sql_queries && msg.runDetails.sql_queries.length > 0 && (
                      <div className="run-detail-section">
                        <strong>🗃️ All SQL Queries:</strong>
                        {msg.runDetails.sql_queries.map((q, qi) => (
                          <pre key={qi} className="sql-block">{q}</pre>
                        ))}
                      </div>
                    )}

                    {msg.runDetails.sql_data_previews &&
                      msg.runDetails.sql_data_previews.some(p => p && p.length > 0) && (
                        <div className="run-detail-section">
                          <strong>📊 Data Preview:</strong>
                          {msg.runDetails.sql_data_previews.map(
                            (preview, pi) =>
                              preview &&
                              preview.length > 0 && (
                                <pre key={pi} className="data-preview-block">
                                  {preview.join('\n')}
                                </pre>
                              ),
                          )}
                        </div>
                      )}
                  </details>
                )}

                {msg.isError && msg.failedMessage && (
                  <button
                    className="retry-btn"
                    onClick={() => retryMessage(msg.failedMessage as string)}
                    disabled={loading}
                  >
                    🔄 Retry
                  </button>
                )}
                {msg.timestamp && (
                  <div className="message-timestamp">
                    {new Date(msg.timestamp).toLocaleTimeString()}
                  </div>
                )}
              </div>
            </div>
          ))}

          {loading && (
            <div className="message message-assistant">
              <div className="message-avatar">🤖</div>
              <div className="message-content">
                <div className="loading-dots">
                  <span></span>
                  <span></span>
                  <span></span>
                </div>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        <div className="input-container">
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={
              chatMode === 'compare'
                ? 'Enter a question to send to both draft and production agents…'
                : chatMode === 'detailed'
                ? 'Ask with detailed run info (SQL queries, data previews)…'
                : 'Ask about customers, churn risk, revenue trends...'
            }
            disabled={loading}
            className="chat-input"
          />
          <button
            onClick={() => sendMessage()}
            disabled={loading || !input.trim()}
            className="send-button"
          >
            Send
          </button>
        </div>
      </div>

      {/* ── Right panel: Power BI ── */}
      <div className="powerbi-panel">
        <div className="powerbi-header">
          <h2>📊 Customer 360 Dashboard</h2>
        </div>

        {pbi.loading && (
          <div className="powerbi-placeholder">
            <div className="loading-dots">
              <span></span>
              <span></span>
              <span></span>
            </div>
            <p style={{ marginTop: '12px' }}>Loading dashboard…</p>
          </div>
        )}

        {!pbi.loading && pbi.error && !pbi.fallbackUrl && (
          <div className="powerbi-placeholder">
            <p style={{ color: '#e74c3c' }}>⚠️ {pbi.error}</p>
            <p className="small-text" style={{ marginTop: '8px' }}>
              A Power BI admin must enable{' '}
              <strong>"Allow service principals to use Power BI APIs"</strong> in the{' '}
              <a
                href="https://app.powerbi.com/admin-portal/tenantSettings"
                target="_blank"
                rel="noreferrer"
                className="inline-link"
              >
                Power BI Admin Portal → Tenant Settings
              </a>{' '}
              for the embedded dashboard to load automatically.
            </p>
            <button className="retry-btn" style={{ marginTop: '12px' }} onClick={fetchPbiToken}>
              🔄 Retry
            </button>
          </div>
        )}

        {/* Fallback: when embed token fails but we have the plain URL, show a
            regular iframe. Works if the user is already signed into Power BI
            in this browser — no admin setting required. */}
        {!pbi.loading && pbi.error && pbi.fallbackUrl && (
          <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
            <div className="powerbi-fallback-banner">
              ⚠️ Embedded token unavailable — showing in browser sign-in mode.{' '}
              <a
                href="https://app.powerbi.com/admin-portal/tenantSettings"
                target="_blank"
                rel="noreferrer"
                className="inline-link"
              >
                Enable service principal API access
              </a>{' '}
              for seamless embedding.{' '}
              <button
                className="retry-btn"
                style={{ display: 'inline', marginTop: 0, marginLeft: '4px' }}
                onClick={fetchPbiToken}
              >
                🔄 Retry token
              </button>
            </div>
            <iframe
              src={pbi.fallbackUrl}
              className="powerbi-iframe"
              title="Customer 360 Dashboard"
              allowFullScreen
            />
          </div>
        )}

        {!pbi.loading && !pbi.error && pbi.config && (
          <PowerBIEmbed embedConfig={pbi.config} cssClassName="powerbi-iframe" />
        )}
      </div>
    </div>
  );
};

export default App;
