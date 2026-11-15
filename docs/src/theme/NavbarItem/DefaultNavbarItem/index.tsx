import type {ReactNode} from 'react';
import OriginalDefaultNavbarItem from '@theme-original/NavbarItem/DefaultNavbarItem';
import type {Props} from '@theme/NavbarItem/DefaultNavbarItem';
import {selectBilingualText, useLanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';

export default function DefaultNavbarItem(props: Props): ReactNode {
  const mode = useLanguageMode();
  const label = typeof props.label === 'string' ? selectBilingualText(props.label, mode) : props.label;
  return <OriginalDefaultNavbarItem {...props} label={label} />;
}
