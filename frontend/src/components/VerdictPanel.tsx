import { useState } from 'react';
import { motion } from 'framer-motion';
import type { VerdictResult, ClassificationCategory, SignalSeverity } from '../types/investigation';
import { CATEGORY_COLORS, SEVERITY_COLORS } from '../types/investigation';

interface VerdictPanelProps {
  verdict: VerdictResult;
}

const CATEGORY_BORDER: Record<ClassificationCategory, string> = {
  LEGITIMATE: 'border-green-400/20',
  SCALPING_VIOLATION: 'border-amber-400/20',
  LIKELY_SCAM: 'border-error/20',
  COUNTERFEIT_RISK: 'border-error/20',
};

const SCAM_CATEGORIES: ClassificationCategory[] = ['LIKELY_SCAM', 'COUNTERFEIT_RISK'];

const SEVERITY_TEXT_COLORS: Record<SignalSeverity, string> = {
  CRITICAL: 'text-error',
  WARNING: 'text-amber-400',
  NEUTRAL: 'text-tertiary',
  CLEAR: 'text-green-400',
};

export function VerdictPanel({ verdict }: VerdictPanelProps) {
  const [reasoningOpen, setReasoningOpen] = useState(false);

  const borderClass = CATEGORY_BORDER[verdict.category];
  const hasShadow = SCAM_CATEGORIES.includes(verdict.category);
  const categoryLabel = verdict.category.replace(/_/g, ' ');

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.5, ease: 'easeOut' }}
      className={`glass rounded-xl p-8 mt-8 border ${borderClass}`}
      style={hasShadow ? { boxShadow: '0 0 40px rgba(255,180,171,0.05)' } : undefined}
    >
      {/* Header row */}
      <div className="flex justify-between items-start">
        {/* Left side */}
        <div>
          <span className="inline-flex items-center gap-1 px-3 py-1 bg-error-container text-on-error-container text-xs uppercase tracking-widest rounded-full">
            <span className="material-symbols-outlined text-sm">warning</span>
            Risk Alert
          </span>
          <h2 className={`text-4xl font-headline font-bold mt-2 ${CATEGORY_COLORS[verdict.category]}`}>
            {categoryLabel}
          </h2>
          <p className="font-mono text-sm text-on-surface-variant mt-1">
            CONFIDENCE_SCORE: {verdict.confidence}%
          </p>
        </div>

        {/* Right side */}
        <div className="flex items-start">
          <button className="border border-error text-error hover:bg-error/10 px-4 py-2 rounded-lg text-sm font-headline">
            Report
          </button>
          <button className="bg-surface-container-highest border border-outline-variant/30 text-on-surface px-4 py-2 rounded-lg text-sm font-headline ml-2">
            Share
          </button>
        </div>
      </div>

      {/* Signal meters */}
      <h3 className="text-xs uppercase tracking-widest text-on-surface-variant mt-8 mb-4">
        RISK INDICATORS
      </h3>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-12 gap-y-6">
        {verdict.signals.map((signal, signalIndex) => (
          <div key={signal.name}>
            <div className="flex items-center justify-between">
              <span className="text-sm font-body text-on-surface">{signal.name}</span>
              <span className={`text-xs uppercase tracking-wider ${SEVERITY_TEXT_COLORS[signal.severity]}`}>
                {signal.severity}
              </span>
            </div>
            <div className="flex gap-1 mt-1.5">
              {Array.from({ length: 5 }).map((_, segmentIndex) => (
                <motion.div
                  key={segmentIndex}
                  className={`h-1.5 flex-1 rounded-sm ${
                    segmentIndex < signal.segmentsFilled
                      ? SEVERITY_COLORS[signal.severity]
                      : 'bg-surface-container'
                  }`}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{
                    delay: signalIndex * 0.1 + segmentIndex * 0.05,
                    duration: 0.3,
                  }}
                />
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Expandable reasoning */}
      <div className="mt-8 border-t border-outline-variant/20 pt-6">
        <button
          onClick={() => setReasoningOpen(!reasoningOpen)}
          className="cursor-pointer flex items-center gap-2 font-headline font-bold text-sm text-on-surface w-full text-left"
        >
          Full Reasoning &amp; Audit Trail
          <span
            className="material-symbols-outlined text-base transition-transform duration-200"
            style={{ transform: reasoningOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}
          >
            expand_more
          </span>
        </button>
        {reasoningOpen && (
          <div className="mt-4 bg-surface-container-lowest rounded-xl p-4 font-mono text-xs text-on-surface-variant leading-relaxed whitespace-pre-wrap">
            {verdict.reasoning}
          </div>
        )}
      </div>
    </motion.div>
  );
}
