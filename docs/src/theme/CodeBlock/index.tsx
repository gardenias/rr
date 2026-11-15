import type {ReactNode} from 'react';
import OriginalCodeBlock from '@theme-original/CodeBlock';
import type {Props} from '@theme/CodeBlock';
import PlantUMLDiagram from '@site/src/components/PlantUMLDiagram';

export default function CodeBlock(props: Props): ReactNode {
  const language = props.className?.replace('language-', '');
  if (language === 'plantuml' && typeof props.children === 'string') {
    return <PlantUMLDiagram source={props.children} />;
  }
  return <OriginalCodeBlock {...props} />;
}
