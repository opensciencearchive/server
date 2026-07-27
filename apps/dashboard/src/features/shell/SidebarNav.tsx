"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { type IconName, icons } from "./icons";
import styles from "./SidebarNav.module.css";

export interface NavItem {
  label: string;
  href: string;
  icon?: IconName;
  /** Count chip, e.g. Builds (14). */
  count?: number;
  children?: Array<{ label: string; href: string }>;
}

export interface NavSection {
  items: NavItem[];
}

function NavLink({ item }: { item: NavItem }) {
  const pathname = usePathname();
  const active = pathname === item.href;

  return (
    <div>
      <Link
        href={item.href}
        className={[styles.item, active ? styles.active : ""].join(" ")}
        aria-current={active ? "page" : undefined}
      >
        {item.icon && <span className={styles.icon}>{icons[item.icon]}</span>}
        <span className={styles.label}>{item.label}</span>
        {item.count !== undefined && (
          <span className={styles.count}>{item.count}</span>
        )}
      </Link>
      {item.children && (
        <div className={styles.children}>
          {item.children.map((child) => {
            const isActive = pathname === child.href;
            return (
              <Link
                key={child.href}
                href={child.href}
                className={[styles.child, isActive ? styles.active : ""].join(" ")}
                aria-current={isActive ? "page" : undefined}
              >
                {child.label}
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function SidebarNav({
  sections,
  footer,
}: {
  sections: NavSection[];
  footer?: React.ReactNode;
}) {
  return (
    <nav className={styles.nav}>
      <div className={styles.sections}>
        {sections.map((section, index) => (
          <div key={index} className={styles.section}>
            {section.items.map((item) => (
              <NavLink key={item.href} item={item} />
            ))}
          </div>
        ))}
      </div>
      {footer && <div className={styles.footer}>{footer}</div>}
    </nav>
  );
}
