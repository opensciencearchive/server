import styles from './DepositWizard.module.css';

interface TemplateStepProps {
  conventionName: string;
  onDownload: () => void;
}

export function TemplateStep({ conventionName, onDownload }: TemplateStepProps) {
  return (
    <div className={styles.panel} key="step-1">
      <div className={styles.panelHeader}>
        <div className={styles.panelNumber}>Step 2</div>
        <h2 className={styles.panelTitle}>Download Template</h2>
        <p className={styles.panelDescription}>
          Download the metadata spreadsheet for <strong>{conventionName}</strong> and fill in your sample information.
        </p>
      </div>
      <div className={styles.templateSection}>
        <div className={styles.templateCard}>
          <div className={styles.templateIcon}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
              <polyline points="14,2 14,8 20,8"/>
              <line x1="8" y1="13" x2="16" y2="13"/>
              <line x1="8" y1="17" x2="16" y2="17"/>
            </svg>
          </div>
          <div className={styles.templateInfo}>
            <p className={styles.templateName}>{conventionName} Template.xlsx</p>
            <p className={styles.templateHint}>Pre-formatted spreadsheet with required and optional fields</p>
          </div>
          <button className={styles.downloadBtn} onClick={onDownload}>
            <svg className={styles.downloadBtnIcon} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M8 2v8M5 7l3 3 3-3"/>
              <path d="M2 12v1.5a.5.5 0 00.5.5h11a.5.5 0 00.5-.5V12"/>
            </svg>
            Download
          </button>
        </div>
        <div className={styles.templateInstructions}>
          <ol>
            <li>Open the template in Excel or Google Sheets</li>
            <li>Fill in one row per sample with your metadata</li>
            <li>Required fields are highlighted — do not remove column headers</li>
            <li>Save as .xlsx and upload in the next step</li>
          </ol>
        </div>
      </div>
    </div>
  );
}
