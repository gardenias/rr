import type {ReactNode} from 'react';
import clsx from 'clsx';
import {ThemeClassNames} from '@docusaurus/theme-common';
import {useSidebarBreadcrumbs} from '@docusaurus/plugin-content-docs/client';
import {useHomePageRoute} from '@docusaurus/theme-common/internal';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import HomeBreadcrumbItem from '@theme/DocBreadcrumbs/Items/Home';
import DocBreadcrumbsStructuredData from '@theme/DocBreadcrumbs/StructuredData';
import {selectBilingualText, useLanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';
import styles from './styles.module.css';

function BreadcrumbsItemLink({
  children,
  href,
  isLast,
}: {
  children: ReactNode;
  href: string | undefined;
  isLast: boolean;
}): ReactNode {
  const className = 'breadcrumbs__link';
  if (isLast) {
    return <span className={className}>{children}</span>;
  }
  return href ? (
    <Link className={className} href={href}>
      <span>{children}</span>
    </Link>
  ) : (
    <span className={className}>{children}</span>
  );
}

export default function DocBreadcrumbs(): ReactNode {
  const breadcrumbs = useSidebarBreadcrumbs();
  const homePageRoute = useHomePageRoute();
  const mode = useLanguageMode();

  if (!breadcrumbs) {
    return null;
  }

  return (
    <>
      <DocBreadcrumbsStructuredData breadcrumbs={breadcrumbs} />
      <nav
        className={clsx(ThemeClassNames.docs.docBreadcrumbs, styles.breadcrumbsContainer)}
        aria-label={translate({
          id: 'theme.docs.breadcrumbs.navAriaLabel',
          message: 'Breadcrumbs',
          description: 'The ARIA label for the breadcrumbs',
        })}>
        <ul className="breadcrumbs">
          {homePageRoute && <HomeBreadcrumbItem />}
          {breadcrumbs.map((item, index) => {
            const isLast = index === breadcrumbs.length - 1;
            const href = item.type === 'category' && item.linkUnlisted ? undefined : item.href;
            return (
              <li
                key={item.href ?? item.label}
                className={clsx('breadcrumbs__item', {'breadcrumbs__item--active': isLast})}>
                <BreadcrumbsItemLink href={href} isLast={isLast}>
                  {selectBilingualText(item.label, mode)}
                </BreadcrumbsItemLink>
              </li>
            );
          })}
        </ul>
      </nav>
    </>
  );
}
