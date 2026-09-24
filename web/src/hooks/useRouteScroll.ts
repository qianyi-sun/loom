import { useLayoutEffect, useRef } from "react";
import { useLocation } from "react-router-dom";

/** Preserve a list's reading position when returning from its details. */
export function useRouteScroll(): void {
  const { pathname, search } = useLocation();
  const positions = useRef(new Map<string, { main: number; window: number }>());
  // Detach the departed route's listener before the new page's layout can clamp
  // scrolling. A passive effect can save that clamped value under the old URL.
  useLayoutEffect(() => {
    const main = document.getElementById("main-content");
    if (!main) return;
    const key = pathname + search;
    const saved = positions.current.get(key) ?? { main: 0, window: 0 };
    let restoring = true;
    const restore = () => {
      if (!restoring) return;
      main.scrollTop = saved.main;
      window.scrollTo(0, saved.window);
      if (main.scrollTop === saved.main && window.scrollY === saved.window) restoring = false;
    };
    const remember = () => {
      if (!restoring) positions.current.set(key, { main: main.scrollTop, window: window.scrollY });
    };
    const stopRestoring = () => { restoring = false; };
    restore();
    // Detail queries can finish after navigation. Restore after their content arrives.
    const observer = new MutationObserver(restore);
    observer.observe(main, { childList: true, subtree: true });
    main.addEventListener("scroll", remember, { passive: true });
    window.addEventListener("scroll", remember, { passive: true });
    window.addEventListener("wheel", stopRestoring, { passive: true });
    window.addEventListener("touchstart", stopRestoring, { passive: true });
    return () => {
      observer.disconnect();
      main.removeEventListener("scroll", remember);
      window.removeEventListener("scroll", remember);
      window.removeEventListener("wheel", stopRestoring);
      window.removeEventListener("touchstart", stopRestoring);
    };
  }, [pathname, search]);
}
