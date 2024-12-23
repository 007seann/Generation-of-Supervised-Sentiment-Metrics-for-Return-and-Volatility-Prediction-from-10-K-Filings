import os
from bs4 import BeautifulSoup
import re
from collections import Counter
import concurrent.futures

#find page break tags and replace with keyword 'split_of_pages'
def find_page_break_tags(soup):
    #first format of page tag
    page_break_tags = soup.find_all('hr', color="#999999")
    #second format of page tag
    if len(page_break_tags) < 6:
        page_break_tags = soup.find_all('hr')
    #third format of page tag - find <div> elements with style 'page-break-after: always'
    if len(page_break_tags) < 6:
        page_break_tags = soup.find_all('div', style=lambda value: (value 
                                                            and re.search(r'page-break-after\s*:\s*always', value, re.IGNORECASE) 
                                                            and 'position:relative' not in value))                        
    #replace page tags with split_of_pages
    for tag in page_break_tags:
        tag.replace_with('split_of_pages')
    return soup

#find heading tags and add [heading][/heading]
def find_headings_with_italic(soup):
    #heading tag with html style bold or special color
    for tag in (soup.find_all(style=lambda value: (value and (
                'font-weight:bold' in value.lower() or
                'font-weight: bold' in value.lower() or
                'font-weight:700' in value.lower() or
                'font-weight: 700' in value.lower() or 
                'color:#0068b5' in value.lower() or
                'font-size:22pt' in value.lower()
    )))):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
                    
                    
    #heading tag b                   
    for tag in soup.find_all('b'):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
    
    #heading tag strong   
    for tag in (soup.find_all('strong')):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
    
    #heading tag em             
    for tag in (soup.find_all('em')):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
    
    #heading tag with html style underline               
    for tag in (soup.find_all(style=lambda value: (value and (
        'text-decoration:underline' in value.lower() or
        'text-decoration: underline' in value.lower())))):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
    
    #heading tag with html style italic 
    for tag in soup.find_all(True, style=True):
        style_value = tag.get('style')  # Get the style attribute value
        if style_value and ('italic' in style_value.lower()):
            content = tag.get_text()
            tag.string = f"\n[heading]{content}[/heading]\n" 
    
    #heading tag i   
    for tag in (soup.find_all('i')):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
        
    return soup

#judge whether the elements near 'split of page' is page footer or not
def remove_page_footer_by_line(text_list,position,min_occurrence,footer_type='text'):
    # get the elements to be deleted by label 'split_of_pages'
    if footer_type == 'length':
        #if forward
        if position<0:
            delete_elements = [len(text_list[i +position]) for i in range(-position,len(text_list)) if text_list[i] == 'split_of_pages']
            # get the content of elements whose occurrences are greater than occurrence
            counter = Counter(delete_elements)
            length_to_delete = [length for length, count in counter.items() if count >= min_occurrence]
            # Iterate in reverse order and delete the corresponding element
            for i in range(len(text_list) - 1, -position-1, -1):
                if text_list[i] == 'split_of_pages' and len(text_list[i +position]) in length_to_delete:
                    if text_list[i +position]!='split_of_pages':
                        del text_list[i +position]
        #if backward
        if position>0:
            delete_elements = [len(text_list[i +position]) for i in range(len(text_list)-position) if text_list[i] == 'split_of_pages']
            # get the content of elements whose occurrences are greater than occurrence
            counter = Counter(delete_elements)
            length_to_delete = [length for length, count in counter.items() if count >= min_occurrence]
            # Iterate in reverse order and delete the corresponding element
            for i in range(len(text_list) -position-1 , -1, -1):
                if text_list[i] == 'split_of_pages' and len(text_list[i +position]) in length_to_delete:
                    if text_list[i +position]!='split_of_pages':
                        del text_list[i +position]
                        
                        
    if footer_type == 'text':
        #if forward
        if position<0:
            delete_elements = [text_list[i +position] for i in range(-position,len(text_list)) if text_list[i] == 'split_of_pages']
            # get the content of elements whose occurrences are greater than occurrence
            counter = Counter(delete_elements)
            texts_to_delete = [text for text, count in counter.items() if count >= min_occurrence]

            # Iterate in reverse order and delete the corresponding element
            for i in range(len(text_list) - 1, -position-1, -1):
                if text_list[i] == 'split_of_pages' and text_list[i +position] in texts_to_delete:
                    if text_list[i +position]!='split_of_pages':
                        del text_list[i +position]
        #if backward
        if position>0:
            delete_elements = [text_list[i +position] for i in range(len(text_list)-position) if text_list[i] == 'split_of_pages']
            # get the content of elements whose occurrences are greater than occurrence
            counter = Counter(delete_elements)
            texts_to_delete = [text for text, count in counter.items() if count >= min_occurrence]

            # Iterate in reverse order and delete the corresponding element
            for i in range(len(text_list) -position-1 , -1, -1):
                if text_list[i] == 'split_of_pages' and text_list[i +position] in texts_to_delete:
                    if text_list[i +position]!='split_of_pages':
                        del text_list[i +position]
    return text_list
    
    #remove page footers by parameters generated from common page footers
def remove_page_footer(text_list):

    # Remove page footer, the third element forward
    text_list = remove_page_footer_by_line(text_list,-3,3,'text')

    # Remove page footer, the second element forward
    text_list = remove_page_footer_by_line(text_list,-2,3,'text')
    
    # Remove page number, the second element forward
    text_list = remove_page_footer_by_line(text_list,-2,5,'length')
    
    # Remove page number, the first element forward
    text_list = remove_page_footer_by_line(text_list,-1,5,'length')
    
    # Remove page footer, backward
    for i in range (6,0,-1):
        text_list = remove_page_footer_by_line(text_list,i,5,'text')
    
    return text_list

#combine nested heading tags e.g. [heading][heading] and [/heading][/heading]
def combine_adjacent_headings(content):
    while re.search(r'\[heading\]\s*\[heading\]', content):
        content = re.sub(r'\[heading\]\s*\[heading\]', '[heading]', content)
    while re.search(r'\[/heading\]\s*\[/heading\]', content):
        content = re.sub(r'\[/heading\]\s*\[/heading\]', '[/heading]', content)
    return content

#check and remove tags for headings that are followed by ','
def replace_heading_with_comma(content):
    pattern_heading = r'\[heading\](.*?)\[/heading\]'
    matches = re.finditer(pattern_heading, content)
    modified_content = content
    
    for match in matches:
        heading_content = match.group(1)
        next_chars_start = match.end()  # Position immediately after [/heading]
        next_chars_end = next_chars_start + 3
        
        if ',' in content[next_chars_start:next_chars_end]:
            to_replace = match.group(0)  # Entire [heading][/heading] pattern
            modified_content = modified_content.replace(to_replace, heading_content)
            
    return modified_content

#remove headings that are not likely to be heading
def filter_illegal_heading(content):
    pattern_heading = r'\[heading\](.*?)\[/heading\]'
    matches = re.finditer(pattern_heading, content)
    modified_content = content
    
    for match in matches:
        heading_content = match.group(1)
        next_chars_start = match.end()  # Position immediately after [/heading]
        
        # Find the first character that's not a space or a newline
        non_whitespace_char = re.search(r'[^\s\n]', content[next_chars_start:])
        
        if non_whitespace_char and non_whitespace_char.group(0) == '[':
            # This is a nested [heading] tag, do nothing
            pass
        elif non_whitespace_char and non_whitespace_char.group(0).islower():
            # Replace the illegal [heading][/heading] pattern with just the heading content
            to_replace = match.group(0)
            modified_content = modified_content.replace(to_replace, heading_content, 1)
            
    return modified_content

#remove headings that do not contain number or character
def process_headings(match):
    content = match.group(1)
    if any(c.isalnum() for c in content):
        return '[heading]'+content+'[/heading]'
    else:
        return ""
    

def process_fillings_for_cik(cik, root_folder, root_folder_fillings):
        read_folder = os.path.join(root_folder, cik)
        save_folder = os.path.join(root_folder_fillings, cik)
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)
        if read_folder == f'{root_folder}/.DS_Store':
            return
        for file in os.listdir(read_folder):
            read_path = os.path.join(read_folder, file)
            
            # Skips if the txt file already exists
            save_path_filling = os.path.join(save_folder, os.path.splitext(file)[0] + '.txt')
            if os.path.exists(save_path_filling):
                continue

            if os.path.splitext(read_path)[1] == '.html':
                with open(read_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                f.close()
                
                #remove irrelevant character
                content = content.replace('&#160;', ' ').replace('&nbsp;', ' ')
                #remove Continued tags
                pattern = r'Item 1A\.(\n|\s)*Risk Factors<\/[a-zA-Z]+>(<\/[a-zA-Z]+>)* <[a-zA-Z]+>\(Continued\)|ITEM 1A\.(.{0,10})RISK FACTORS (.{0,10})\(continued\)'
                content = re.sub(pattern, '', content, flags=re.IGNORECASE)
                
                soup = BeautifulSoup(content, 'html.parser') #parse html
                
                soup = find_page_break_tags(soup) #replace page break tags with 'split_of_pages'
                
                soup = find_headings_with_italic(soup) #split headings
                
                #remove page footers
                text_list = [text for text in soup.stripped_strings]
                if text_list[-1] != 'split_of_pages':
                    text_list.append('split_of_pages')
                text_list = remove_page_footer(text_list)
                text_list = [text for text in text_list if text != 'split_of_pages']
                content = ' '.join(text_list)
                
                content = combine_adjacent_headings(content) #remove nested tags
                
                #remove headings that do not contain number or alphabet
                pattern = r'\[heading\](.*?)\[/heading\]'
                content = re.sub(pattern, process_headings, content, flags=re.DOTALL)
                
                content = replace_heading_with_comma(content) #remove heading followed by comma
                content = filter_illegal_heading(content) #remove illegal heading
                
                save_path_filling = os.path.join(save_folder, os.path.splitext(file)[0] + '.txt')
                with open(save_path_filling, 'w', encoding='utf-8') as f:
                    f.write(content)
                f.close()
                
            

def executor(data_folder, save_folder):
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for cik in os.listdir(data_folder):
            future = executor.submit(process_fillings_for_cik, cik, data_folder, save_folder)
            futures.append(future)
            
            
        # Wait for all tasks to complete
        for future in futures:
            future.result()
        
        # All tasks are completed, shutdown the executor
        executor.shutdown()


# root_folder = 'data'
# root_folder_fillings = 'fillings'


# with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
#     futures = []
#     for cik in os.listdir(root_folder):
#         future = executor.submit(process_fillings_for_cik, cik)
#         futures.append(future)
        
        
#     # Wait for all tasks to complete
#     for future in futures:
#         future.result()
    
#     # All tasks are completed, shutdown the executor
#     executor.shutdown()