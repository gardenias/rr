import type {ReactNode} from 'react';
import OriginalLayout from '@theme-original/DocItem/Layout';
import type {Props} from '@theme/DocItem/Layout';
import ReadingProgress from '@site/src/components/ReadingProgress';

export default function DocItemLayout(props: Props): ReactNode {
  return (
    <>
      <ReadingProgress />
      <OriginalLayout {...props} />
    </>
  );
}
