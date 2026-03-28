import { motion, AnimatePresence } from 'framer-motion';
import type { StepWithState } from '../types/investigation';
import { StepCard } from './StepCard';

interface InvestigationTimelineProps {
  steps: StepWithState[];
}

export function InvestigationTimeline({ steps }: InvestigationTimelineProps) {
  // Only render steps up to and including the last non-pending step
  const lastNonPendingIndex = steps.reduce(
    (acc, step, i) => (step.status !== 'pending' ? i : acc),
    -1
  );

  if (lastNonPendingIndex < 0) return null;

  const visibleSteps = steps.slice(0, lastNonPendingIndex + 1);

  return (
    <section>
      <h2 className="text-xs uppercase tracking-[0.2em] text-on-surface-variant font-body mb-6">
        Investigation Timeline
      </h2>

      <div className="relative space-y-6">
        {/* Vertical line connecting circles */}
        <div className="absolute left-[27px] top-0 bottom-0 w-px bg-outline-variant/30" />

        <AnimatePresence>
          {visibleSteps.map((step, index) => (
            <motion.div
              key={step.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4, delay: index * 0.1, ease: 'easeOut' }}
            >
              <StepCard step={step} />
            </motion.div>
          ))}
        </AnimatePresence>
      </div>
    </section>
  );
}
