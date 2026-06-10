/**
 * Input + Textarea primitives. Match the Card / Button look (soft
 * slate borders, focus-glow). Inputs forward all native props.
 */
import { forwardRef, type InputHTMLAttributes, type TextareaHTMLAttributes } from "react";

import { cn } from "../lib/cn";

const FIELD_BASE =
  "block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800 placeholder:text-slate-400 disabled:cursor-not-allowed disabled:opacity-60";

export const Input = forwardRef<
  HTMLInputElement,
  InputHTMLAttributes<HTMLInputElement>
>(function Input({ className, ...rest }, ref) {
  return <input ref={ref} className={cn(FIELD_BASE, className)} {...rest} />;
});

export const Textarea = forwardRef<
  HTMLTextAreaElement,
  TextareaHTMLAttributes<HTMLTextAreaElement>
>(function Textarea({ className, ...rest }, ref) {
  return (
    <textarea
      ref={ref}
      className={cn(FIELD_BASE, "font-mono leading-relaxed", className)}
      {...rest}
    />
  );
});
