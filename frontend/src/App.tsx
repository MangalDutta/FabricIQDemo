import React, { useState } from 'react';
import axios from 'axios';
import './App.css';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp?: string;
}

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || '';
const POWERBI_REPORT_URL = import.meta.env.VITE_POWERBI_REPORT_URL || '';

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);

  const sampleQuestions = [
    'Top 5 customers by LifetimeValue in Maharashtra',
    'Which customers are high churn risk?',
    'Show revenue trend for Karnataka',
    'Count customers by State',
    'Average ChurnRiskScore by Segment'
  ];

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
      const errorMessage: Message = {
        role: 'assistant',
        content: `Error: ${error.response?.data?.detail || error.message || 'Failed to get response'}`,
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
          <h1>🤖 Customer 360 AI Analytics</h1>
          <p>Ask questions about your customer data</p>
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
                <div className="message-text">{msg.content}</div>
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
        {POWERBI_REPORT_URL ? (
          <iframe
            src={POWERBI_REPORT_URL}
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
