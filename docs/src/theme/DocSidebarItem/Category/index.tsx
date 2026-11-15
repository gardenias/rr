import type {ReactNode} from 'react';
import OriginalCategory from '@theme-original/DocSidebarItem/Category';
import type {Props} from '@theme/DocSidebarItem/Category';
import {selectBilingualText, useLanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';

export default function DocSidebarItemCategory(props: Props): ReactNode {
  const mode = useLanguageMode();
  const item = {...props.item, label: selectBilingualText(props.item.label, mode)};
  return <OriginalCategory {...props} item={item} />;
}
