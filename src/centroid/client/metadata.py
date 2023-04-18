import xml.etree.ElementTree as ElementTree
from typing import List

nsmap = {
    'edmx': 'http://docs.oasis-open.org/odata/ns/edmx',
    'edm': 'http://docs.oasis-open.org/odata/ns/edm'
}


def get_annotation_string(element: ElementTree, annotation: str) -> str or None:
    child = element.find(f'edm:Annotation[@Term="{annotation}"]', nsmap)
    if child is not None:
        return child.get('String')
    return None


def get_annotation_bool(element: ElementTree, annotation: str) -> bool or None:
    child = element.find(f'edm:Annotation[@Term="{annotation}"]', nsmap)
    if child is not None:
        return bool(child.get('Bool')) or bool(child.get('Tag'))
    return None


class EdmPropertyRef:
    Name: str = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        return self


class EdmKey:
    PropertyRef: List[EdmPropertyRef] = []

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        children = element.findall('edm:PropertyRef', nsmap)
        self.PropertyRef = list(map(lambda x: EdmPropertyRef().__readxml__(x), children))
        return self


class EdmAnnotation:
    Term: str = None
    String: str = None
    Tag = None
    Bool = None

    def __readxml__(self, element: ElementTree):
        self.Term = element.get('Term')
        self.String = element.get('String')
        if 'Tag' in element.attrib:
            self.Tag = element.get('Tag')
        if 'Bool' in element.attrib:
            self.Bool = bool(element.get('Bool'))
        return self


class EdmProperty:
    Name: str = None
    Type: str = None
    Nullable: bool = True
    Computed: bool = False
    Immutable: bool = False
    Description: str = None
    LongDescription: str = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.Type = element.get('Type')
        self.Nullable = bool(element.get('Nullable')) if element.get('Nullable') is not None else True

        self.Computed = get_annotation_bool(element, 'Org.OData.Core.V1.Computed')
        self.Immutable = get_annotation_bool(element, 'Org.OData.Core.V1.Immutable')

        self.Description = get_annotation_string(element, 'Org.OData.Core.V1.Description')
        self.LongDescription = get_annotation_string(element, 'Org.OData.Core.V1.LongDescription')

        return self


class EdmNavigationProperty:
    Name: str = None
    Type: str = None
    Description: str = None
    LongDescription: str = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.Type = element.get('Type')
        self.Description = get_annotation_string(element, 'Org.OData.Core.V1.Description')
        self.LongDescription = get_annotation_string(element, 'Org.OData.Core.V1.LongDescription')
        return self


class EdmParameter:
    Name: str = None
    Type: str = None
    Nullable: bool = True

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.Type = element.get('Type')
        self.Nullable = bool(element.get('Nullable'))
        return self


class EdmReturnType:
    Type: str = None
    Nullable: bool = True

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Type = element.get('Type')
        self.Nullable = bool(element.get('Nullable'))
        return self


class EdmProcedure:
    Name: str = None
    IsBound = True
    Parameter: List[EdmParameter] = []
    ReturnType: EdmReturnType = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.IsBound = bool(element.get('IsBound'))
        params = element.findall('edm:Parameter', nsmap)
        self.Parameter = list(map(lambda x: EdmParameter().__readxml__(x), params))
        return_type = element.find('edm:ReturnType', nsmap)
        if return_type is not None:
            self.ReturnType = EdmReturnType().__readxml__(return_type)
        return self


class EdmFunction(EdmProcedure):

    def __init__(self):
        super().__init__()

    def __readxml__(self, element: ElementTree):
        return super().__readxml__(element)


class EdmAction(EdmProcedure):

    def __init__(self):
        super().__init__()

    def __readxml__(self, element: ElementTree):
        return super().__readxml__(element)


class EdmEntityType:
    Name: str = None
    BaseType: str = None
    OpenType = True
    Key: EdmKey = None
    Property: List[EdmProperty] = []
    NavigationProperty: List[EdmNavigationProperty] = []
    ImplementsType: str = None
    Annotations = []

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.OpenType = bool(element.get('OpenType')) if element.get('OpenType') is not None else False
        # get base type
        if element.get('BaseType') is not None:
            self.BaseType = element.get('BaseType')
        implements = element.find('edm:Annotation[@Term="DataModel.OData.Core.V1.Implements"]', nsmap)
        if implements is not None:
            self.ImplementsType = implements.get('String')
        # get primary key
        key = element.find('Key', nsmap)
        if key is not None:
            self.Key = EdmKey().__readxml__(key)
        # get properties
        elements = element.findall('edm:Property', nsmap)
        self.Property = list(map(lambda x: EdmProperty().__readxml__(x), elements))
        # get navigation properties
        elements = element.findall('edm:NavigationProperty', nsmap)
        self.NavigationProperty = list(map(lambda x: EdmNavigationProperty().__readxml__(x), elements))
        # get annotations
        elements = element.findall('edm:Annotation', nsmap)
        self.Annotations = list(map(lambda x: EdmAnnotation().__readxml__(x), elements))
        return self


class EdmEntitySet:
    Name: str = None
    EntityType: str = None
    ResourcePath: str = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        self.Name = element.get('Name')
        self.EntityType = element.get('EntityType')
        self.ResourcePath = get_annotation_string(element, 'Org.OData.Core.V1.ResourcePath')
        return self


class EdmEntityContainer:
    EntitySet = []

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        elements = element.findall('edm:EntitySet', nsmap)
        self.EntitySet = list(map(lambda x: EdmEntitySet().__readxml__(x), elements))
        return self


class EdmSchema:
    Action = []
    Function = []
    EntityType = []
    EntityContainer: EdmEntityContainer = None

    def __init__(self):
        pass

    def __readxml__(self, element: ElementTree):
        container = element.find('edm:EntityContainer', nsmap)
        # set entity container
        if container is not None:
            self.EntityContainer = EdmEntityContainer().__readxml__(container)
        # get entity types
        elements = element.findall('edm:EntityType', nsmap)
        self.EntityType = list(map(lambda x: EdmEntityType().__readxml__(x), elements))
        # get actions
        elements = element.findall('edm:Action', nsmap)
        self.Action = list(map(lambda x: EdmAction().__readxml__(x), elements))
        # get functions
        elements = element.findall('edm:Function', nsmap)
        self.Function = list(map(lambda x: EdmFunction().__readxml__(x), elements))
        return self
