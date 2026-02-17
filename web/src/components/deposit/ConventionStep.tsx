import type { Convention } from '@/types';
import styles from './DepositWizard.module.css';

interface ConventionStepProps {
  conventions: Convention[];
  selectedSrn: string | null;
  onSelect: (srn: string) => void;
  loading: boolean;
}

export function ConventionStep({ conventions, selectedSrn, onSelect, loading }: ConventionStepProps) {
  return (
    <div className={styles.panel} key="step-0">
      <div className={styles.panelHeader}>
        <div className={styles.panelNumber}>Step 1</div>
        <h2 className={styles.panelTitle}>Choose a Convention</h2>
        <p className={styles.panelDescription}>
          Select the data convention that matches your dataset. This determines the metadata schema and validation rules.
        </p>
      </div>
      {loading ? (
        <div className={styles.conventionGrid}>
          <p className={styles.panelDescription}>Loading conventions...</p>
        </div>
      ) : (
        <div className={styles.conventionGrid}>
          {conventions.map(conv => (
            <div
              key={conv.srn}
              className={`${styles.conventionCard} ${selectedSrn === conv.srn ? styles.selected : ''}`}
              onClick={() => onSelect(conv.srn)}
            >
              <div className={styles.conventionRadio} />
              <div className={styles.conventionInfo}>
                <h3 className={styles.conventionName}>{conv.title}</h3>
                <p className={styles.conventionDesc}>{conv.description}</p>
                <div className={styles.conventionMeta}>
                  <span className={styles.conventionTag}>{conv.schema_srn}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
