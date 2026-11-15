import type {ReactNode} from 'react';
import OriginalLink from '@theme-original/DocSidebarItem/Link';
import type {Props} from '@theme/DocSidebarItem/Link';
import {selectBilingualText, useLanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';

export default function DocSidebarItemLink(props: Props): ReactNode {
  const mode = useLanguageMode();
  const item = {...props.item, label: selectBilingualText(props.item.label, mode)};
  return <OriginalLink {...props} item={item} />;
}
