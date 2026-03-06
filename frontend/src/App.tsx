import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './App.css';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp?: string;
}

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || '';

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  // Power BI URL: fetched from the backend at runtime so it can be updated
  // via an App Service setting without rebuilding the Docker image.
  // Falls back to the build-time env var for local development.
  const [powerbiReportUrl, setPowerbiReportUrl] = useState<string>(
    import.meta.env.VITE_POWERBI_REPORT_URL || ''
  );

  useEffect(() => {
    if (!BACKEND_URL) return;
    axios
      .get(`${BACKEND_URL}/api/config`)
      .then((resp) => {
        const url: string = resp.data?.powerbi_report_url || '';
        if (url) setPowerbiReportUrl(url);
      })
      .catch(() => {
        // Config fetch failed — keep the build-time VITE_ value as fallback
      });
  }, []);

  const sampleQuestions = [
    'Top 5 customers by LifetimeValue in Maharashtra',
    'Which customers have ChurnRiskScore above 80?',
    'Show average MonthlyRevenue by State for Karnataka and Tamil Nadu',
    'Count customers by Segment',
    'List Startup customers in Delhi with LifetimeValue above 50000'
  ];

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const resetConversation = async () => {
    try {
      await axios.post(`${BACKEND_URL}/api/reset`, { userId: 'web-user' });
    } catch (err) {
      console.warn('Reset request failed (clearing local state anyway):', err);
    }
    setMessages([]);
    setInput('');
  };

  const sendMessage = async () => {
    if (!input.trim()) return;

    const userMessage: Message = {
      role: 'user',
      content: input,
      timestamp: new Date().toISOString()
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      const response = await axios.post(`${BACKEND_URL}/api/chat`, {
        message: input,
        userId: 'web-user'
      });

      const assistantMessage: Message = {
        role: 'assistant',
        content: response.data.answer || 'No response received.',
        timestamp: response.data.timestamp
      };

      setMessages(prev => [...prev, assistantMessage]);
    } catch (error: any) {
      const detail = error.response?.data?.detail;
      let errorText: string;

      if (detail && typeof detail === 'object' && detail.error === 'agent_not_ready') {
        // Structured error from backend — show troubleshooting steps
        const steps = (detail.troubleshooting || []) as string[];
        errorText =
          '⚠️ The AI Agent is not ready yet.\n\n' +
          (steps.length > 0
            ? 'To fix this:\n' + steps.map((s: string, i: number) => `${i + 1}. ${s}`).join('\n')
            : detail.message || 'Please check the backend /api/debug endpoint for details.');
      } else {
        errorText = `Error: ${typeof detail === 'string' ? detail : error.message || 'Failed to get response'}`;
      }

      const errorMessage: Message = {
        role: 'assistant',
        content: errorText,
        timestamp: new Date().toISOString()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const useSampleQuestion = (question: string) => {
    setInput(question);
  };

  return (
    <div className="app-container">
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

        <div className="messages-container">
          {messages.length === 0 && (
            <div className="welcome-message">
              <h2>Welcome! 👋</h2>
              <p>Ask me anything about your customer data. Try these questions:</p>
              <div className="sample-questions">
                {sampleQuestions.map((q, idx) => (
                  <button
                    key={idx}
                    className="sample-question-btn"
                    onClick={() => useSampleQuestion(q)}
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg, idx) => (
            <div key={idx} className={`message message-${msg.role}`}>
              <div className="message-avatar">
                {msg.role === 'user' ? '👤' : '🤖'}
              </div>
              <div className="message-content">
                <div className="message-text" style={{ whiteSpace: 'pre-wrap' }}>{msg.content}</div>
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
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about customers, churn risk, revenue trends..."
            disabled={loading}
            className="chat-input"
          />
          <button
            onClick={sendMessage}
            disabled={loading || !input.trim()}
            className="send-button"
          >
            Send
          </button>
        </div>
      </div>

      <div className="powerbi-panel">
        <div className="powerbi-header">
          <h2>📊 Customer 360 Dashboard</h2>
        </div>
        {powerbiReportUrl ? (
          <iframe
            src={powerbiReportUrl}
            title="Customer 360 Power BI Report"
            className="powerbi-iframe"
          />
        ) : (
          <div className="powerbi-placeholder">
            <p>Power BI report will appear here</p>
            <p className="small-text">Configure VITE_POWERBI_REPORT_URL to enable</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default App;
