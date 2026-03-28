interface TabItem {
  icon: string;
  label: string;
  active?: boolean;
}

const TABS: TabItem[] = [
  { icon: 'radar', label: 'Scan', active: true },
  { icon: 'folder_open', label: 'Cases' },
  { icon: 'trending_up', label: 'Trends' },
  { icon: 'settings', label: 'Settings' },
];

export function MobileBottomNav() {
  return (
    <nav className="md:hidden fixed bottom-0 left-0 right-0 h-16 z-50 bg-[#0b1326]/90 backdrop-blur-lg border-t border-outline-variant/20 flex justify-around items-center">
      {TABS.map((tab) => (
        <button
          key={tab.label}
          className={`flex flex-col items-center gap-1 ${
            tab.active ? 'text-primary' : 'text-on-surface-variant'
          }`}
        >
          <span
            className="material-symbols-outlined text-xl"
            style={tab.active ? { fontVariationSettings: "'FILL' 1" } : undefined}
          >
            {tab.icon}
          </span>
          <span className="text-[10px] font-bold">{tab.label}</span>
        </button>
      ))}
    </nav>
  );
}
