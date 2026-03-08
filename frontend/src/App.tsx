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
}

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || '';
const CHAT_TIMEOUT_MS = 120_000; // 2 minutes — matches backend Fabric query timeout

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const [agentStatus, setAgentStatus] = useState<AgentStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);

  // Power BI embed state — driven by a backend-generated embed token so users
  // don't need to be signed into Power BI in their browser.
  const [pbi, setPbi] = useState<PbiEmbedState>({
    loading: true,
    config: null,
    error: '',
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
      setPbi({ loading: false, config: null, error: 'Backend URL not configured.' });
      return;
    }
    setPbi(prev => ({ ...prev, loading: true, error: '' }));
    try {
      const resp = await axios.get(`${BACKEND_URL}/api/powerbi-token`, { timeout: 20_000 });
      const { embed_token, embed_url, report_id } = resp.data;

      setPbi({
        loading: false,
        error: '',
        config: {
          type: 'report',
          id: report_id,
          embedUrl: embed_url,
          accessToken: embed_token,
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
      setPbi({ loading: false, config: null, error: msg });
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
  };

  // ── Chat ────────────────────────────────────────────────────────────────────
  const sendMessage = async (overrideMessage?: string) => {
    const messageText = overrideMessage || input;
    if (!messageText.trim()) return;

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
      const response = await axios.post(
        `${BACKEND_URL}/api/chat`,
        { message: messageText, userId: 'web-user' },
        { signal: controller.signal },
      );

      const assistantMessage: Message = {
        role: 'assistant',
        content: response.data.answer || 'No response received.',
        timestamp: response.data.timestamp,
      };
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
          checkAgentStatus();
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
        </div>

        {agentNotReady && (
          <div className="status-banner status-banner-warning">
            <div className="status-banner-content">
              <span className="status-banner-icon">⚠️</span>
              <div className="status-banner-text">
                <strong>AI Agent Not Ready</strong>
                <p>{agentStatus!.message}</p>

                {/* Auto-publish button — tries to publish the agent via API */}
                <div style={{ marginTop: '8px', display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  <button
                    className="retry-btn"
                    onClick={tryPublishAgent}
                    disabled={publishState.loading}
                    title="Attempt to publish the Fabric Data Agent using the backend Managed Identity"
                  >
                    {publishState.loading ? '⏳ Publishing…' : '🚀 Try Auto-Publish Agent'}
                  </button>
                </div>

                {/* Result of last publish attempt */}
                {publishState.result && (
                  <p style={{ marginTop: '6px', fontSize: '0.85em' }}>
                    <TextWithLinks text={publishState.result} />
                    {publishState.portalUrl && (
                      <>
                        {' '}—{' '}
                        <a
                          href={publishState.portalUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-link"
                        >
                          Open in Fabric portal →
                        </a>
                      </>
                    )}
                  </p>
                )}

                {agentStatus!.troubleshooting.length > 0 && (
                  <details className="status-banner-details" style={{ marginTop: '8px' }}>
                    <summary>Manual setup instructions</summary>
                    <ol>
                      {agentStatus!.troubleshooting.map((step, i) => (
                        <li key={i}>
                          <TextWithLinks text={step} />
                        </li>
                      ))}
                    </ol>
                  </details>
                )}
              </div>
              <button
                className="status-banner-refresh"
                onClick={checkAgentStatus}
                title="Recheck status"
              >
                🔄
              </button>
            </div>
          </div>
        )}

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
                    title={agentNotReady ? 'AI Agent is not ready — see banner above' : ''}
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
              agentNotReady
                ? 'AI Agent is not ready — see banner above for details'
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

        {!pbi.loading && pbi.error && (
          <div className="powerbi-placeholder">
            <p style={{ color: '#e74c3c' }}>⚠️ {pbi.error}</p>
            <p className="small-text" style={{ marginTop: '8px' }}>
              If the error mentions "Allow service principals to use Power BI APIs", a Power BI
              admin must enable that setting in the{' '}
              <a
                href="https://app.powerbi.com/admin-portal/tenantSettings"
                target="_blank"
                rel="noreferrer"
              >
                Power BI Admin Portal → Tenant Settings
              </a>
              .
            </p>
            <button className="retry-btn" style={{ marginTop: '12px' }} onClick={fetchPbiToken}>
              🔄 Retry
            </button>
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
