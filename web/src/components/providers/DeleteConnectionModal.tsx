import { DestructiveActionDialog } from "../DestructiveActionDialog";

export type DeleteConnectionModalProps = {
  connectionName: string;
  error?: unknown | null;
  onClose: () => void;
  onSubmit: () => Promise<void>;
  pending?: boolean;
};

export default function DeleteConnectionModal({
  connectionName,
  error = null,
  onClose,
  onSubmit,
  pending = false,
}: DeleteConnectionModalProps): JSX.Element {
  return (
    <DestructiveActionDialog
      open
      onClose={onClose}
      title="Delete connection"
      target={connectionName}
      consequence="The connection will be soft-deleted and unavailable for new trials. Existing trial attribution and history remain."
      confirmLabel="Delete connection"
      pendingLabel="Deleting…"
      confirmation={{
        type: "typed",
        expected: connectionName,
        inputLabel: `Type connection name to confirm: ${connectionName}`,
      }}
      pending={pending}
      error={error}
      onConfirm={onSubmit}
    />
  );
}
