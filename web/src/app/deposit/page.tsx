import { DepositWizard } from '@/components/deposit/DepositWizard';

export const metadata = {
  title: 'Deposit Data — Open Science Archive',
  description: 'Submit your research data to the Open Science Archive.',
};

export default function DepositPage() {
  return (
    <main>
      <DepositWizard />
    </main>
  );
}
