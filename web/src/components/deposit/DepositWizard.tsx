'use client';

import { useState, useCallback, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import styles from './DepositWizard.module.css';
import { ConventionStep } from './ConventionStep';
import { TemplateStep } from './TemplateStep';
import { MetadataStep } from './MetadataStep';
import { DataFilesStep } from './DataFilesStep';
import { osa } from '@/lib/sdk';
import { useAuth } from '@/hooks/useAuth';
import type { Convention } from '@/types';

const STEPS = ['Convention', 'Template', 'Metadata', 'Data Files'];

export function DepositWizard() {
  const router = useRouter();
  const { isAuthenticated, isLoading, login } = useAuth();
  const [currentStep, setCurrentStep] = useState(0);
  const [conventions, setConventions] = useState<Convention[]>([]);
  const [conventionsLoading, setConventionsLoading] = useState(true);
  const [selectedConventionSrn, setSelectedConventionSrn] = useState<string | null>(null);
  const [metadataFile, setMetadataFile] = useState<File | null>(null);
  const [dataFiles, setDataFiles] = useState<File[]>([]);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastVisible, setToastVisible] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    osa.deposition.listConventions().then(res => {
      setConventions(res.items);
      setConventionsLoading(false);
    });
  }, []);

  const selectedConvention = conventions.find(c => c.srn === selectedConventionSrn);

  const showToast = useCallback((message: string) => {
    setToastMessage(message);
    setToastVisible(true);
    setTimeout(() => setToastVisible(false), 2500);
    setTimeout(() => setToastMessage(null), 3000);
  }, []);

  const canProceed = () => {
    switch (currentStep) {
      case 0: return selectedConventionSrn !== null;
      case 1: return true;
      case 2: return metadataFile !== null;
      case 3: return dataFiles.length > 0;
      default: return false;
    }
  };

  const handleNext = () => {
    if (currentStep < 3) setCurrentStep(prev => prev + 1);
  };

  const handleBack = () => {
    if (currentStep > 0) setCurrentStep(prev => prev - 1);
  };

  const handleDownloadTemplate = async () => {
    if (!selectedConventionSrn) return;
    const blob = await osa.deposition.downloadTemplate(selectedConventionSrn);
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedConvention?.title ?? 'Template'}.xlsx`;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Template downloaded: ${selectedConvention?.title ?? 'Convention'}.xlsx`);
  };

  const handleSubmit = async () => {
    if (!selectedConventionSrn || submitting) return;
    setSubmitting(true);
    try {
      const { srn } = await osa.deposition.create(selectedConventionSrn);
      if (metadataFile) {
        await osa.deposition.uploadSpreadsheet(srn, metadataFile);
      }
      for (const file of dataFiles) {
        await osa.deposition.uploadFile(srn, file);
      }
      await osa.deposition.submit(srn);
      router.push('/deposition/' + encodeURIComponent(srn));
    } finally {
      setSubmitting(false);
    }
  };

  if (isLoading) {
    return (
      <div className={styles.page}>
        <div className={styles.container} />
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className={styles.page}>
        <div className={styles.container}>
          <div className={styles.header}>
            <h1 className={styles.title}>Sign in to deposit data</h1>
            <p className={styles.subtitle}>
              You need to be signed in to submit research data to the Open Science Archive.
            </p>
          </div>
          <div className={styles.actions}>
            <div />
            <div>
              <button className={styles.nextBtn} onClick={login}>
                Sign in with ORCiD
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.page}>
      <div className={styles.container}>
        {/* Header */}
        <div className={styles.header}>
          <div className={styles.breadcrumb}>
            <span>OSA</span>
            <span className={styles.breadcrumbSep}>/</span>
            <span>New Deposition</span>
          </div>
          <h1 className={styles.title}>Deposit Data</h1>
          <p className={styles.subtitle}>
            Submit your research data to the Open Science Archive.
            Choose a convention, fill in metadata, and upload your files.
          </p>
        </div>

        {/* Stepper */}
        <div className={styles.stepper}>
          {STEPS.map((label, i) => (
            <div
              key={label}
              className={`${styles.step} ${i === currentStep ? styles.active : ''} ${i < currentStep ? styles.completed : ''}`}
              onClick={() => i < currentStep && setCurrentStep(i)}
            >
              <div className={styles.stepDot}>
                {i < currentStep ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                    <path d="M2.5 6L5 8.5L9.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                ) : (
                  i + 1
                )}
              </div>
              <span className={styles.stepLabel}>{label}</span>
            </div>
          ))}
        </div>

        {/* Step panels */}
        {currentStep === 0 && (
          <ConventionStep
            conventions={conventions}
            selectedSrn={selectedConventionSrn}
            onSelect={setSelectedConventionSrn}
            loading={conventionsLoading}
          />
        )}

        {currentStep === 1 && (
          <TemplateStep
            conventionName={selectedConvention?.title ?? ''}
            onDownload={handleDownloadTemplate}
          />
        )}

        {currentStep === 2 && (
          <MetadataStep
            conventionName={selectedConvention?.title ?? ''}
            file={metadataFile}
            onFileChange={(file) => {
              setMetadataFile(file);
              showToast('Metadata spreadsheet added');
            }}
            onFileRemove={() => setMetadataFile(null)}
          />
        )}

        {currentStep === 3 && (
          <DataFilesStep
            files={dataFiles}
            onFilesAdd={(newFiles) => {
              setDataFiles(prev => [...prev, ...newFiles]);
              showToast(`${newFiles.length} file${newFiles.length > 1 ? 's' : ''} added`);
            }}
            onFileRemove={(idx) => setDataFiles(prev => prev.filter((_, i) => i !== idx))}
          />
        )}

        {/* Navigation */}
        <div className={styles.actions}>
          <div>
            {currentStep > 0 && (
              <button className={styles.backBtn} onClick={handleBack}>
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M8.5 3L4.5 7l4 4"/>
                </svg>
                Back
              </button>
            )}
          </div>
          <div>
            {currentStep < 3 ? (
              <button
                className={styles.nextBtn}
                disabled={!canProceed()}
                onClick={handleNext}
              >
                Continue
                <svg className={styles.nextBtnIcon} viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M5.5 3l4 4-4 4"/>
                </svg>
              </button>
            ) : (
              <button
                className={styles.submitBtn}
                disabled={!canProceed() || submitting}
                onClick={handleSubmit}
              >
                <svg className={styles.submitBtnIcon} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M16 6l-8.5 8.5L4 11"/>
                </svg>
                {submitting ? 'Submitting...' : 'Submit Deposition'}
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Toast */}
      {toastMessage && (
        <div className={`${styles.toast} ${toastVisible ? styles.visible : ''}`}>
          <svg className={styles.toastIcon} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 8l3 3 5-5"/>
          </svg>
          {toastMessage}
        </div>
      )}

    </div>
  );
}
