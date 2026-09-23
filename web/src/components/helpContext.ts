import { createContext } from "react";
import type { HelpTopicId } from "../lib/helpContent";

export const HelpContext = createContext<((topic: HelpTopicId) => void) | null>(null);
