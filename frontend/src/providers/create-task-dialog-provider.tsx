import { createContext, useContext, useState, ReactNode } from "react";
import CreateTaskDialogRHF from "../components/CreateTask";

type Ctx = {
  openCreateTask: () => void;
  closeCreateTask: () => void;
};
const CreateTaskDialogCtx = createContext<Ctx | null>(null);

export function CreateTaskDialogProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const openCreateTask = () => setOpen(true);
  const closeCreateTask = () => setOpen(false);

  return (
    <CreateTaskDialogCtx.Provider value={{ openCreateTask, closeCreateTask }}>
      {children}
      {/* Mount the dialog once at app root */}
      <CreateTaskDialogRHF open={open} onOpenChange={setOpen} />
    </CreateTaskDialogCtx.Provider>
  );
}

export function useCreateTaskDialog() {
  const ctx = useContext(CreateTaskDialogCtx);
  if (!ctx) throw new Error("useCreateTaskDialog must be used inside CreateTaskDialogProvider");
  return ctx;
}