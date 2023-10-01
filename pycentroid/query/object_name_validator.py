import re


class InvalidObjectNameError(Exception):
    def __init__(self):
        self.message = 'Invalid database object name.'
        super().__init__(self.message)


class ValidatorPatterns:

    Default = '([a-zA-Z0-9_]+)'
    Latin = '([\u0030-\u0039\u0041-\u005A\u0061-\u007A\u005F]+)'
    LatinExtended = '([\u0030-\u0039\u0041-\u005A\u0061-\u007A\u00A0-\u024F\u005F]+)'
    Greek = '([\u0030-\u0039\u0041-\u005A\u0061-\u007A\u0386-\u03CE\u005F]+)'
    Cyrillic = '([\u0030-\u0039\u0041-\u007A\u0061-\u007A\u0400-\u04FF\u005F]+)'
    Hebrew = '([\u0030-\u0039\u0041-\u005A\u0061-\u007A\u05D0-\u05F2\u005F]+)'


class ObjectNameValidator:
    def __init__(self, pattern=ValidatorPatterns.Default):
        self.pattern = pattern
        self.qualified_pattern = f'\\*$|^{pattern}((\\.|\\/){pattern})*(\\.\\*)?$'

    def test(self, name, qualified=True, throw_error=True):
        if qualified:
            result = re.match(self.qualified_pattern, name)
        else:
            result = re.match(f'^{self.pattern}$', name)
        if result is not None:
            return True
        if throw_error:
            raise InvalidObjectNameError()
        return False
    
    def escape(self, name, format_string=r'\1'):
        """Escapes a database object name based on the given format
        Args:
            name (str): An object name expression to escape
            format_string (str, optional): Object name format expression

        Returns:
                str: An object name expression to escape
        """
        self.test(name)
        return re.sub(self.pattern, format_string, name)
