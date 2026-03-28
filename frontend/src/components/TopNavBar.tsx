const NAV_LINKS = ['Dashboard', 'History', 'Threat Intel', 'Settings'];

export function TopNavBar() {
  return (
    <header className="fixed top-0 left-0 right-0 h-16 z-50 glass border-b border-outline-variant/15 shadow-[0_8px_32px_rgba(175,198,255,0.06)]">
      <div className="flex items-center justify-between h-full px-4 lg:px-6">
        {/* Left: Logo + Nav */}
        <div className="flex items-center gap-8">
          <div className="flex items-center gap-2">
            <span className="material-symbols-outlined text-primary text-2xl">radar</span>
            <span className="font-headline font-bold text-primary text-lg">FraudFish</span>
          </div>
          <nav className="hidden md:flex items-center gap-6">
            {NAV_LINKS.map((link) => (
              <button
                key={link}
                className="text-on-surface-variant hover:text-on-surface text-sm font-body transition-colors"
              >
                {link}
              </button>
            ))}
          </nav>
        </div>

        {/* Right: Actions */}
        <div className="flex items-center gap-3">
          <button className="text-on-surface-variant hover:text-on-surface transition-colors">
            <span className="material-symbols-outlined text-xl">notifications</span>
          </button>
          <button className="text-on-surface-variant hover:text-on-surface transition-colors">
            <span className="material-symbols-outlined text-xl">help</span>
          </button>
          <div className="w-8 h-8 rounded-full bg-surface-container-highest" />
        </div>
      </div>
    </header>
  );
}
