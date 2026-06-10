/**
 * Button primitive. Three visual variants:
 *   - `primary` — accent fill, used for the dominant action on a page
 *   - `secondary` — white surface + slate border, used for everything else
 *   - `danger` — red fill for destructive confirms
 *
 * Forwards all native button attributes (type, disabled, onClick, etc.)
 * so call sites compose normally.
 */
import { forwardRef, type ButtonHTMLAttributes } from "react";

import { cn } from "../lib/cn";

export type ButtonVariant = "primary" | "secondary" | "danger";
export type ButtonSize = "sm" | "md";

const VARIANT_CLASSES: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-white border-accent hover:bg-accent-hover hover:border-accent-hover",
  secondary:
    "bg-white text-slate-700 border-slate-200 hover:bg-slate-50 hover:border-slate-300",
  danger:
    "bg-red-600 text-white border-red-600 hover:bg-red-700 hover:border-red-700",
};

const SIZE_CLASSES: Record<ButtonSize, string> = {
  sm: "px-2.5 py-1 text-xs",
  md: "px-3.5 py-2 text-sm",
};

export interface ButtonProps
  extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  function Button(
    { variant = "secondary", size = "md", className, type = "button", ...rest },
    ref,
  ) {
    return (
      <button
        ref={ref}
        type={type}
        className={cn(
          "inline-flex items-center justify-center rounded-lg border font-medium",
          "disabled:cursor-not-allowed disabled:opacity-50",
          VARIANT_CLASSES[variant],
          SIZE_CLASSES[size],
          className,
        )}
        {...rest}
      />
    );
  },
);
