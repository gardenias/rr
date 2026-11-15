import type {ReactNode} from 'react';
import OriginalPaginatorNavLink from '@theme-original/PaginatorNavLink';
import type {Props} from '@theme/PaginatorNavLink';
import {selectBilingualText, useLanguageMode} from '@site/src/components/LanguageModeSwitcher/languageMode';

export default function PaginatorNavLink(props: Props): ReactNode {
  const mode = useLanguageMode();
  const title = typeof props.title === 'string' ? selectBilingualText(props.title, mode) : props.title;
  const subLabel = props.subLabel
    ? selectBilingualText(props.isNext ? '下一页 / Next' : '上一页 / Previous', mode)
    : props.subLabel;
  return <OriginalPaginatorNavLink {...props} title={title} subLabel={subLabel} />;
}
