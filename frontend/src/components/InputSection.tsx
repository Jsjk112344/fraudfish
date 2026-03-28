import { useState } from 'react';

type InputMode = 'investigate' | 'scan' | 'dashboard';

interface InputSectionProps {
  onSubmit: (url: string) => void;
  onScan?: (eventName: string) => void;
  onDashboard?: () => void;
  isRunning: boolean;
  onCancel: () => void;
  activeMode?: InputMode;
  onModeChange?: (mode: InputMode) => void;
}

const SAMPLE_URL = 'https://www.carousell.sg/p/f1-singapore-gp-2026-pit-grandstand-1234567890';

const DEMO_EVENTS = [
  'F1 Singapore GP 2026',
  'DAY6 10th Anniversary',
  '(G)I-DLE Syncopation',
  'Eric Chou',
];

function isValidUrl(input: string): boolean {
  try {
    const url = new URL(input);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

export function InputSection({
  onSubmit,
  onScan = () => {},
  onDashboard = () => {},
  isRunning,
  onCancel,
  activeMode = 'investigate',
  onModeChange = () => {},
}: InputSectionProps) {
  const [input, setInput] = useState('');
  const [error, setError] = useState<string | null>(null);

  function switchMode(mode: InputMode) {
    if (mode === activeMode) return;
    if (isRunning) onCancel();
    setInput('');
    setError(null);
    onModeChange(mode);
  }

  function handleInvestigateSubmit(inputUrl?: string) {
    const target = inputUrl ?? input;
    if (!isValidUrl(target)) {
      setError('Could not detect listing. Please paste a valid Carousell, Viagogo, or marketplace URL.');
      return;
    }
    setError(null);
    onSubmit(target);
  }

  function handleScanSubmit(eventName?: string) {
    const target = eventName ?? input;
    if (!target.trim()) {
      setError('Please enter an event name to scan.');
      return;
    }
    setError(null);
    onScan(target.trim());
  }

  function handleSubmit(value?: string) {
    if (activeMode === 'investigate') {
      handleInvestigateSubmit(value);
    } else {
      handleScanSubmit(value);
    }
  }

  function handleSampleUrl() {
    setInput(SAMPLE_URL);
    setError(null);
    handleInvestigateSubmit(SAMPLE_URL);
  }

  function handleDemoEvent(eventName: string) {
    setInput(eventName);
    setError(null);
    handleScanSubmit(eventName);
  }

  const isDashboard = activeMode === 'dashboard';

  const TAB_CONFIG: { mode: InputMode; label: string; icon: string }[] = [
    { mode: 'investigate', label: 'Investigate', icon: 'search' },
    { mode: 'scan', label: 'Scan Event', icon: 'radar' },
    { mode: 'dashboard', label: 'Dashboard', icon: 'dashboard' },
  ];

  const MODE_DESCRIPTIONS: Record<InputMode, string> = {
    investigate:
      'Input a listing URL to start an autonomous investigation. Our AI agent will analyze the seller, verify the event, and deliver a fraud verdict with full evidence.',
    scan:
      'Enter an event name to scan all major marketplaces. Our AI agent will find listings, investigate each one, and produce a threat intelligence report.',
    dashboard:
      'Proactive threat intelligence. Our agent discovers the highest-profile events in Singapore, ranks them by fraud risk, and scans for suspicious listings — automatically.',
  };

  return (
    <div className="max-w-4xl mx-auto text-center">
      {/* Tab Toggle */}
      <div className="flex gap-2 justify-center mb-6">
        {TAB_CONFIG.map(({ mode, label, icon }) => (
          <button
            key={mode}
            onClick={() => switchMode(mode)}
            className={`px-4 py-2 rounded-lg font-headline font-bold text-sm transition-colors flex items-center gap-1.5 ${
              activeMode === mode
                ? 'text-primary border-b-2 border-primary bg-surface-container-high/50'
                : 'text-on-surface-variant border-b-2 border-transparent hover:text-on-surface hover:bg-surface-container/50'
            }`}
          >
            <span className="material-symbols-outlined text-lg">{icon}</span>
            {label}
          </button>
        ))}
      </div>

      {/* Headline */}
      <h1 className="text-4xl font-headline font-bold text-primary leading-tight">
        AI Fraud Detective for Ticket Scams
      </h1>
      <p className="mt-4 text-on-surface-variant font-body max-w-2xl mx-auto">
        {MODE_DESCRIPTIONS[activeMode]}
      </p>

      {/* Input Group (hidden in dashboard mode — it has its own CTA) */}
      {!isDashboard && (
        <>
          <div className="mt-8 group relative">
            {/* Outer glow */}
            <div className="absolute inset-0 bg-gradient-to-r from-primary/20 to-tertiary/20 rounded-lg blur-xl opacity-0 group-hover:opacity-50 transition duration-1000" />

            {/* Inner input container */}
            <div className="relative bg-surface-container-low rounded-lg p-2 flex items-center gap-2 border border-outline-variant/20 focus-within:border-primary/50 transition-colors">
              <span className="material-symbols-outlined text-outline ml-2">
                {activeMode === 'investigate' ? 'link' : 'search'}
              </span>
              <input
                type="text"
                value={input}
                onChange={(e) => {
                  setInput(e.target.value);
                  setError(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !isRunning) handleSubmit();
                }}
                placeholder={
                  activeMode === 'investigate'
                    ? 'Paste a Carousell, Viagogo, or marketplace URL...'
                    : 'Enter an event name, e.g. F1 Singapore GP 2026...'
                }
                className="flex-1 bg-transparent outline-none text-on-surface placeholder:text-outline font-body text-sm"
              />
              {isRunning ? (
                <button
                  onClick={onCancel}
                  className="border border-error text-error px-6 py-2.5 rounded-lg font-headline font-bold text-sm hover:bg-error/10 transition-colors whitespace-nowrap"
                >
                  {activeMode === 'investigate' ? 'Stop Investigation' : 'Stop Scan'}
                </button>
              ) : (
                <button
                  onClick={() => handleSubmit()}
                  className="bg-primary-container text-on-primary px-6 py-2.5 rounded-lg font-headline font-bold text-sm hover:brightness-110 transition whitespace-nowrap"
                >
                  {activeMode === 'investigate' ? 'Investigate Listing' : 'Scan Event'}
                </button>
              )}
            </div>
          </div>

          {/* Error message */}
          {error && (
            <p className="mt-2 text-error text-sm font-body">{error}</p>
          )}

          {/* Quick action chips */}
          <div className="mt-4 flex items-center justify-center gap-3 flex-wrap">
            {activeMode === 'investigate' ? (
              <button
                onClick={handleSampleUrl}
                disabled={isRunning}
                className="rounded-full bg-surface-container-highest border border-outline-variant/30 hover:border-primary/50 px-4 py-1.5 text-xs text-on-surface-variant font-body flex items-center gap-2 transition-colors disabled:opacity-50"
              >
                <span className="material-symbols-outlined text-sm">play_circle</span>
                Sample listing
              </button>
            ) : (
              DEMO_EVENTS.map((eventName) => (
                <button
                  key={eventName}
                  onClick={() => handleDemoEvent(eventName)}
                  disabled={isRunning}
                  className="rounded-full bg-surface-container-highest border border-outline-variant/30 hover:border-primary/50 px-4 py-1.5 text-xs text-on-surface-variant font-body flex items-center gap-2 transition-colors disabled:opacity-50"
                >
                  <span className="material-symbols-outlined text-sm">event</span>
                  {eventName}
                </button>
              ))
            )}
          </div>
        </>
      )}

      {/* Dashboard CTA */}
      {isDashboard && (
        <div className="mt-8">
          {isRunning ? (
            <button
              onClick={onCancel}
              className="border border-error text-error px-8 py-3 rounded-lg font-headline font-bold text-sm hover:bg-error/10 transition-colors"
            >
              Stop Scanning
            </button>
          ) : (
            <button
              onClick={onDashboard}
              className="bg-primary-container text-on-primary px-8 py-3 rounded-lg font-headline font-bold text-sm hover:brightness-110 transition flex items-center gap-2 mx-auto"
            >
              <span className="material-symbols-outlined">satellite_alt</span>
              Discover Threats
            </button>
          )}
        </div>
      )}
    </div>
  );
}
