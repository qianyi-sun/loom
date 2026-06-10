/**
 * Card — surface primitive with optional header. Replaces the
 * `loom-card` div pattern that ran through the old SPA. Use
 * `<Card.Header>` / `<Card.Body>` / `<Card.Footer>` slots when the
 * card needs structure; otherwise just nest children directly.
 *
 * Heading discipline: `Card.Header`'s `headingLevel` chooses the
 * semantic tag. Pages with a page-level `<h1>` should use h2/h3 here
 * so the heading hierarchy stays sane for screen readers.
 */
import { forwardRef, type HTMLAttributes } from "react";

import { cn } from "../lib/cn";

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  children: React.ReactNode;
}

const CardRoot = forwardRef<HTMLDivElement, CardProps>(function CardRoot(
  { children, className, ...rest },
  ref,
) {
  return (
    <div
      ref={ref}
      className={cn("glass-card overflow-hidden", className)}
      {...rest}
    >
      {children}
    </div>
  );
});

type HeadingTag = "h2" | "h3" | "h4";

interface CardHeaderProps {
  title: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
  /**
   * Semantic heading tag for the title. Defaults to `h3` (Card sits
   * under a page-level `h1` and section `h2`). Set to `h2` when a
   * Card is the dominant heading on a screen.
   */
  headingLevel?: HeadingTag;
}

function CardHeader({
  title,
  description,
  actions,
  className,
  headingLevel = "h3",
}: CardHeaderProps): JSX.Element {
  const Heading = headingLevel;
  return (
    <div
      className={cn(
        "flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4",
        className,
      )}
    >
      <div className="min-w-0">
        <Heading className="text-sm font-semibold text-slate-800">
          {title}
        </Heading>
        {description ? (
          <p className="mt-1 text-xs text-slate-500">{description}</p>
        ) : null}
      </div>
      {actions ? <div className="shrink-0">{actions}</div> : null}
    </div>
  );
}

function CardBody({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}): JSX.Element {
  return <div className={cn("p-5", className)}>{children}</div>;
}

function CardFooter({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}): JSX.Element {
  return (
    <div
      className={cn(
        "border-t border-slate-200 bg-slate-50/50 px-5 py-3",
        className,
      )}
    >
      {children}
    </div>
  );
}

export const Card = Object.assign(CardRoot, {
  Header: CardHeader,
  Body: CardBody,
  Footer: CardFooter,
});
