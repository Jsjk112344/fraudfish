interface NavItem {
  icon: string;
  label: string;
  active?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { icon: 'radar', label: 'Live Scan', active: true },
  { icon: 'folder_open', label: 'Case Files' },
  { icon: 'person_search', label: 'Entity Search' },
  { icon: 'monitoring', label: 'Market Watch' },
  { icon: 'api', label: 'API Access' },
];

const BOTTOM_ITEMS: NavItem[] = [
  { icon: 'dns', label: 'System Status' },
  { icon: 'description', label: 'Documentation' },
];

export function Sidebar() {
  return (
    <aside className="hidden lg:flex flex-col fixed left-0 top-16 w-64 h-[calc(100vh-64px)] bg-surface-container-low border-r border-outline-variant/15 z-40">
      {/* Agent Identity */}
      <div className="p-5 border-b border-outline-variant/15">
        <div className="flex items-center gap-3 mb-4">
          <div className="w-10 h-10 rounded-full bg-surface-container-highest flex items-center justify-center">
            <span className="material-symbols-outlined text-primary">radar</span>
          </div>
          <div>
            <div className="font-headline font-bold text-on-surface text-sm">FraudFish Core</div>
            <div className="text-xs text-on-surface-variant">AI Fraud Detective</div>
          </div>
        </div>
        <button className="bg-primary-container text-on-primary w-full rounded-lg py-2.5 font-headline font-bold text-sm hover:brightness-110 transition">
          New Investigation
        </button>
      </div>

      {/* Nav Items */}
      <nav className="flex-1 py-3">
        {NAV_ITEMS.map((item) => (
          <button
            key={item.label}
            className={`flex items-center gap-3 w-full px-5 py-3 text-sm transition-transform duration-150 ${
              item.active
                ? 'bg-surface-variant border-l-4 border-primary text-primary'
                : 'text-on-surface-variant hover:translate-x-1 hover:bg-[#1c253d]'
            }`}
          >
            <span
              className="material-symbols-outlined text-xl"
              style={item.active ? { fontVariationSettings: "'FILL' 1" } : undefined}
            >
              {item.icon}
            </span>
            <span className="font-body">{item.label}</span>
          </button>
        ))}
      </nav>

      {/* Bottom Section */}
      <div className="border-t border-outline-variant/15 py-3">
        {BOTTOM_ITEMS.map((item) => (
          <button
            key={item.label}
            className="flex items-center gap-3 w-full px-5 py-2.5 text-sm text-on-surface-variant hover:translate-x-1 hover:bg-[#1c253d] transition-transform duration-150"
          >
            <span className="material-symbols-outlined text-xl">{item.icon}</span>
            <span className="font-body">{item.label}</span>
          </button>
        ))}
      </div>
    </aside>
  );
}
