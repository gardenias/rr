import OriginalComponentTypes from '@theme-original/NavbarItem/ComponentTypes';
import type {ComponentTypesObject} from '@theme/NavbarItem/ComponentTypes';
import LanguageModeSwitcher from '@site/src/components/LanguageModeSwitcher';

const ComponentTypes: ComponentTypesObject = {
  ...OriginalComponentTypes,
  'custom-languageModeSwitcher': LanguageModeSwitcher,
};

export default ComponentTypes;
