import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Activity, AlertTriangle, ArrowUpRight, BrainCircuit, IndianRupee, RefreshCw, Send, ShoppingBag, Users } from 'lucide-react';

type Kpis = { revenue: number; orders: number; active_customers: number; aov: number; basket_size: number; anomalies: number };
type Event = { event_id: string; user_id: string; amount: number; category: string; location: string; timestamp: string; algorithm: string; risk_score: number; risk_level: string };
type Category = { category: string; revenue: number; orders: number };

const API = '/api';
const money = (value: number) => `₹${value.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;

function App() {
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [events, setEvents] = useState<Event[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState('');
  const [form, setForm] = useState({ user_id: 'U-INPUT', product_id: 'P-INPUT', amount: '2499', quantity: '1', category: 'Electronics', location: 'Bangalore', device: 'web', algorithm: 'isolation_forest' });
  const socketRef = useRef<WebSocket | null>(null);

  async function refresh() {
    setLoading(true);
    const [kpiResponse, eventsResponse, categoryResponse] = await Promise.all([
      fetch(`${API}/kpis`), fetch(`${API}/events?limit=8`), fetch(`${API}/categories`),
    ]);
    setKpis(await kpiResponse.json());
    setEvents(await eventsResponse.json());
    setCategories(await categoryResponse.json());
    setLoading(false);
  }

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 10000);
    const socketUrl = import.meta.env.DEV ? 'ws://localhost:8000/ws' : `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws`;
    const socket = new WebSocket(socketUrl);
    socketRef.current = socket;

    socket.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === 'refresh') refresh();
      if (payload.type === 'connected') console.info(payload.message);
    };

    return () => {
      socket.close();
      window.clearInterval(timer);
    };
  }, []);

  async function submitEvent(event: FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    setMessage('');
    const response = await fetch(`${API}/ingest`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
      event_id: `INPUT-${Date.now()}`, event_type: 'purchase', ...form, amount: Number(form.amount), quantity: Number(form.quantity), timestamp: new Date().toISOString(), risk_score: 0, risk_level: 'LOW',
    }) });
    setMessage(response.ok ? 'Stream event accepted' : 'Could not submit event');
    setSubmitting(false);
    if (response.ok) refresh();
  }

  const cards = kpis ? [
    { label: 'Revenue captured', value: money(kpis.revenue), icon: IndianRupee, tone: 'mint' },
    { label: 'Purchase orders', value: kpis.orders.toLocaleString(), icon: ShoppingBag, tone: 'blue' },
    { label: 'Active customers', value: kpis.active_customers.toLocaleString(), icon: Users, tone: 'amber' },
    { label: 'Risk signals', value: kpis.anomalies.toLocaleString(), icon: AlertTriangle, tone: 'coral' },
  ] : [];

  return <main className="shell">
    <header className="topbar"><div className="brand"><span className="brand-mark"><Activity size={20} /></span><span>StreamPulse</span></div><div className="status"><span className="pulse" /> LIVE PIPELINE <button className="icon-button" onClick={refresh} aria-label="Refresh dashboard"><RefreshCw size={16} className={loading ? 'spin' : ''} /></button></div></header>
    <section className="intro"><div><p className="eyebrow">REAL-TIME COMMERCE INTELLIGENCE</p><h1>Signal, at the speed of business.</h1><p className="lede">A live operating picture of revenue, customers, and risk across your event stream.</p></div><div className="updated">Auto-refreshing every 10 seconds<br /><span>Kafka → Spark ML → PostgreSQL</span></div></section>
    <section className="metric-grid">{cards.map(({ label, value, icon: Icon, tone }) => <article className={`metric ${tone}`} key={label}><div className="metric-icon"><Icon size={18} /></div><p>{label}</p><strong>{value}</strong><span className="metric-trend"><ArrowUpRight size={13} /> streaming now</span></article>)}</section>
    <section className="input-panel panel"><div className="panel-head"><div><p className="eyebrow">INGEST A STREAM</p><h2>Add an event to the pipeline</h2></div><BrainCircuit size={22} className="panel-symbol" /></div><form className="event-form" onSubmit={submitEvent}><label>Customer ID<input value={form.user_id} onChange={event => setForm({ ...form, user_id: event.target.value })} /></label><label>Product ID<input value={form.product_id} onChange={event => setForm({ ...form, product_id: event.target.value })} /></label><label>Amount (INR)<input type="number" min="0" value={form.amount} onChange={event => setForm({ ...form, amount: event.target.value })} /></label><label>Quantity<input type="number" min="1" value={form.quantity} onChange={event => setForm({ ...form, quantity: event.target.value })} /></label><label>Category<select value={form.category} onChange={event => setForm({ ...form, category: event.target.value })}><option>Electronics</option><option>Fashion</option><option>Home</option><option>Grocery</option></select></label><label>Location<select value={form.location} onChange={event => setForm({ ...form, location: event.target.value })}><option>Bangalore</option><option>Mumbai</option><option>Delhi</option><option>Hyderabad</option></select></label><label>Device<select value={form.device} onChange={event => setForm({ ...form, device: event.target.value })}><option>web</option><option>mobile</option><option>tablet</option></select></label><label>ML algorithm<select value={form.algorithm} onChange={event => setForm({ ...form, algorithm: event.target.value })}><option value="isolation_forest">Isolation Forest</option><option value="kmeans">K-Means</option><option value="zscore">Z-score</option><option value="heuristic">Risk heuristic</option></select></label><button className="submit-button" type="submit" disabled={submitting}><Send size={15} /> {submitting ? 'Sending...' : 'Send to stream'}</button>{message && <span className="form-message">{message}</span>}</form></section>
    <section className="content-grid"><article className="panel activity-panel"><div className="panel-head"><div><p className="eyebrow">EVENT STREAM</p><h2>Latest transactions</h2></div><span className="live-tag"><span className="pulse" /> LIVE</span></div><div className="table-wrap"><table><thead><tr><th>Customer</th><th>Category</th><th>Location</th><th>Value</th><th>ML prediction</th></tr></thead><tbody>{events.map(event => <tr key={event.event_id}><td><strong>{event.user_id}</strong><small>{event.event_id}</small></td><td>{event.category}</td><td>{event.location}</td><td className="amount">{money(event.amount)}</td><td><span className={`risk ${event.risk_level.toLowerCase()}`}>{event.risk_level} · {event.risk_score.toFixed(3)}</span><small className="algorithm">{event.algorithm.replace('_', ' ')}</small></td></tr>)}</tbody></table></div></article><aside className="panel category-panel"><div className="panel-head"><div><p className="eyebrow">MIX BY CATEGORY</p><h2>Revenue shape</h2></div></div><div className="category-list">{categories.map((item, index) => { const max = categories[0]?.revenue || 1; return <div className="category-row" key={item.category}><div className="category-label"><span>{item.category}</span><strong>{money(item.revenue)}</strong></div><div className="bar-track"><div className={`bar bar-${index}`} style={{ width: `${Math.max(8, item.revenue / max * 100)}%` }} /></div><small>{item.orders} orders</small></div>})}</div><div className="insight"><span className="insight-icon"><Activity size={16} /></span><p><strong>Pipeline healthy</strong><br />Events are being processed in real time.</p></div></aside></section>
  </main>;
}

export default App;
