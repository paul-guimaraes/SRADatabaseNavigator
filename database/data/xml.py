from multiprocessing import cpu_count
from lxml import etree
from .util import format_name
from threading import Lock
from concurrent.futures import ThreadPoolExecutor


lock = Lock()


# Gera a estrutura de tabelas correpondentes seguindo a lógica de banco de dados relacional
def generate_structure(element: etree.ElementTree, structure: dict, references: dict, paths: dict, step=0,
                       start_node=None):
    if element.getroottree().getelementpath(element) == '.':
        name = format_name(element.tag)
    else:
        name = format_name(element.getroottree().getelementpath(element))
    if start_node == element.tag:
        structure[name] = {'internal_id': {'type': 'BIGINT', 'origin': 'internal'}}
    # atributos da tabela como conteúdo da tag
    if element.text is not None:
        lock.acquire()
        if name not in structure:
            structure[name] = {}
        structure[name][format_name(element.tag)] = {'type': 'TEXT', 'origin': 'text'}
        lock.release()
    # atributos da tabela como atributos da tag
    for attrib in element.attrib:
        lock.acquire()
        if name not in structure:
            structure[name] = {}
        structure[name][format_name(attrib)] = {'type': 'TEXT', 'origin': 'attrib'}
        lock.release()
    # verificando se existem tags que podem ser inseridas como atributos da estrutura
    for child_element in element:
        if len(child_element) == 0 and len(child_element.attrib) == 0:
            if name not in structure:
                lock.acquire()
                structure[name] = {}
                lock.release()
    # montando referências
    if name in structure and name not in references:
        reference = element.getparent()
        while reference is not None:
            if element.getroottree().getelementpath(reference) == '.':
                referenced_name = format_name(reference.tag)
            else:
                referenced_name = format_name(element.getroottree().getelementpath(reference))
            if referenced_name in structure:
                lock.acquire()
                references[name] = referenced_name
                lock.release()
                break
            reference = reference.getparent()
    # incluindo path
    if name in structure:
        lock.acquire()
        paths[element.getroottree().getelementpath(element)] = name
        lock.release()
    # percorrendo aninhamentos utilizando recursão
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        for child_element in element:
            if name not in structure or len(child_element) > 0 or len(child_element.attrib) > 0:
                # only firts node childs are processing in thread mode
                if start_node == element.tag:
                    executor.submit(generate_structure,
                                    *(child_element, structure, references, paths, step+1, start_node))
                else:
                    generate_structure(child_element, structure, references, paths, step+1, start_node)
            elif child_element.text is None:
                try:
                    child_element_name = format_name(child_element.tag)
                except Exception as err:
                    print(err, 'Failed to extract element structure.', 'Parent:',
                          element.getroottree().getelementpath(element), 'Child:', child_element, flush=True)
                    child_element_name = None
                if child_element_name is not None:
                    if child_element_name not in structure[name]:
                        lock.acquire()
                        structure[name][child_element_name] = {
                            'type': 'TEXT', 'origin': 'tag_empty', 'tag': child_element.tag}
                        lock.release()
            else:
                lock.acquire()
                try:
                    structure[name][format_name(child_element.tag)] = {
                        'type': 'TEXT', 'origin': 'tag', 'tag': child_element.tag}
                except Exception as err:
                    print(err, 'Failed to extract element structure.', 'Parent:',
                          element.getroottree().getelementpath(element), 'Child:', child_element, flush=True)
                lock.release()
