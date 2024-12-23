import datetime
import numpy as np 
import pandas as pd 
from ratelimit import limits, sleep_and_retry
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import concurrent.futures  # Import the concurrent.futures module
from collections import Counter
import re
import csv

import nltk
nltk.data.path.append('/opt/airflow/nltk_data')
# nltk.download('punkt')
from nltk.tokenize import word_tokenize

# from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize

# nltk.download('stopwords')
stop_words = set(nltk.corpus.stopwords.words('english'))



#try to find page numbers in txt format reports
def contains_only_numbers(row):
    for cell in row:
        if not re.match(r'^\d+$', cell):
            return False
    return True

#try to find headings that consist of all capital letters
def capital_sentence_detect(content):
    sentences = content.split('.')
    modified_content =''
    for sentence in sentences:
        if sentence.isupper():
            sentence = f"[heading]{sentence}[/heading]"
        modified_content += sentence + " "
        
    return modified_content.strip()
    
### functions for HTML format reports
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
def find_headings(soup):
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
                
    #heading tag with html style underline
    for tag in (soup.find_all(style=lambda value: (value and (
        'text-decoration:underline' in value.lower() or
        'text-decoration: underline' in value.lower())))):
        content = tag.get_text()
        tag.string = f"\n[heading]{content}[/heading]\n"
        
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

#combine nested heading tags e.g. [heading][heading] and [/heading][/heading]
def combine_adjacent_headings(content):
    while re.search(r'\[heading\]\s*\[heading\]', content):
        content = re.sub(r'\[heading\]\s*\[heading\]', '[heading]', content)
    while re.search(r'\[/heading\]\s*\[/heading\]', content):
        content = re.sub(r'\[/heading\]\s*\[/heading\]', '[/heading]', content)
    return content

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

#get the position of keyword, keyword must be in [heading][/heading] tags
def find_heading_positions_with_keyword(content, keyword_list):
    heading_positions = []
    pattern = r'\[heading\](.*?)\[/heading\]'
    headings = re.findall(pattern, content, re.DOTALL)
    
    for heading in headings:
        if len(heading) < 100:
            for keyword in keyword_list:
                keyword = re.sub(r'\W+', '', keyword).lower()
                alphanumeric_text = re.sub(r'\W+', '', heading).lower()
                if keyword in alphanumeric_text:
                    heading_start = content.find(f'[heading]{heading}[/heading]')
                    heading_end = heading_start + len(f'[heading]{heading}[/heading]')
                    heading_positions.append((heading_start, heading_end))
            
    return heading_positions

#get the pairs of position that are most likely to be correct
def find_max_difference_pair(first_heading_position, second_heading_position):
    try:
        max_difference = 0
        max_difference_pair = None

        for first_heading in first_heading_position:
            valid_second_headings = [second_heading for second_heading in second_heading_position if second_heading[0] > first_heading[0]]
            if not valid_second_headings:
                continue
            
            closest_start = min(valid_second_headings, key=lambda x: x[0])
            difference = closest_start[0] - first_heading[0]
            
            if difference > max_difference and difference > 1000:  # Limit difference to be larger than 1000
                max_difference = difference
                max_difference_pair = (first_heading[1], closest_start[0])

        return max_difference_pair, None  # Return None for error status

    except Exception as e:
        return None, str(e)

#remove headings that do not contain number or character
def process_headings(match):
    content = match.group(1)
    if any(c.isalnum() for c in content):
        return '[heading]'+content+'[/heading]'
    else:
        return ""
    
#check the first heading, if it is 'item1A risk factors', remove heading
def remove_heading_risk_factors(content):
    pattern = r'\[heading\](.*?)\[/heading\]'
    match = re.search(pattern, content, re.DOTALL)  # Find the first pair of [heading][/heading]
    if match:
        heading_content = match.group(1)
        if 'riskfactors' in re.sub(r'\W+', '', heading_content).lower():  # Check if 'riskfactors' is in the heading text
            content = content.replace(match.group(0), '')  # Remove the entire [heading][/heading] pair
    return content

#remove heading that contained '(Continued)' or '(continued)'
def remove_heading_risk_factors_continued(content):
    pattern = r'\[heading\](.*?)\[/heading\]'
    matches = re.findall(pattern, content, re.DOTALL)
    for match in matches:
        heading_content = match
        if re.search(r'\([Cc]ontinued\)', heading_content):  # Check if '(Continued)' or '(continued)' is in the heading text
            content = content.replace(f'[heading]{match}[/heading]', '')  # Remove the entire [heading][/heading] pair
    return content



#find titles, split titles from headings
def find_title(elements):
    new_elements = []
    i = 0
    while i < len(elements):
        element = elements[i]
        if '[heading]' in element and '[/heading]' in element:
            heading_text = re.search(r'\[heading\](.*?)\[/heading\]', element).group(1)
            
            words = re.findall(r'\b\w+\b', heading_text)
            non_stop_words = [word for word in words if word.lower() not in stop_words]
            all_start_with_capital = all((word[0].isupper() or word[0].isdigit()) for word in non_stop_words) and len(non_stop_words)>=1
            
            if all_start_with_capital and i + 1 < len(elements) and '[heading]' in elements[i + 1]:
                new_element = element.replace('[heading]', '[title]').replace('[/heading]', '[/title]')
                new_elements.append(new_element)
            else:
                new_elements.append(element)
        else:
            new_elements.append(element)
        i += 1
    return new_elements

#split titles from headings
def split_title_heading_content(content):
    # Extract content before the first [heading] tag
    before_first_heading_pattern = r'^(.*?)(?=\[heading\])'
    before_first_heading_match = re.search(before_first_heading_pattern, content, re.DOTALL)

    # Extract content between [heading] and [/heading] tags along with non-heading content
    heading_content_pattern = r'(\[heading\].*?\[/heading\])\s*(.*?)(?=\[heading\]|\Z)'
    heading_matches = re.findall(heading_content_pattern, content, re.DOTALL)

    combined_contents = []
    
    # Add content before the first [heading] tag if found
    if before_first_heading_match:
        combined_contents.append(before_first_heading_match.group(1).strip())

    for heading_content, non_heading_content in heading_matches:
        combined_contents.append(heading_content)
        if non_heading_content.strip():
            combined_contents.append(non_heading_content)

    # Process and join the content
    content = find_title(combined_contents)  
    content = ' '.join(content)
    return content

# Regular expression pattern to match [heading] tags and their content

def process_heading_pairs(match):
    content = match.group(1)
    word_count = len(word_tokenize(content))  # Count words using word_tokenize
    if word_count < 10:
        return content
    else:
        return f"[/heading]{content}[heading]"
    
def filter_content_between_heading_pairs(content):
    # Replace the matched content using the process_heading function
    pattern = r'\[/heading\](.*?)\[heading\]'
    content = re.sub(r'\s+', ' ', content)
    content = re.sub(pattern, process_heading_pairs, content)
    content = content.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')
    return content

#remove headings that are not likely to be heading
def remove_short_heading(content):
    pattern_heading = r'\[heading\](.*?)\[/heading\]'
    # Replace short [heading][/heading] pairs with their content
    matches = re.finditer(pattern_heading, content)
    for match in matches:
        heading_content = match.group(1)
        words_in_heading = word_tokenize(heading_content)  # Tokenize words using NLTK
        # Define a regex pattern to match only symbols
        symbol_pattern = r'^[^\w]+$'

        # Filter out tokens that consist only of symbols using regex
        words_in_heading = [word for word in words_in_heading if not re.match(symbol_pattern, word)]

        # Check if the heading contains specific phrases
        if ('FORWARD-LOOKING STATEMENTS' in heading_content.upper() or
            'FORWARD-LOOKING STATEMENTS' in words_in_heading):
            continue  # Skip filtering if the phrase is found

        if len(words_in_heading) <=1:
            full_pair = match.group(0)  # The entire [heading][/heading] pair
            content = content.replace(full_pair, heading_content)
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

#error log function
def save_error_info(cik, file_name, error_csv_path):
    error_info = [cik, file_name]
    # Ensure the directory exists
    dir_name = os.path.dirname(error_csv_path)
    os.makedirs(dir_name, exist_ok=True)  # Create all intermediate directories if they don't exist
    with open(error_csv_path, 'a', newline='', encoding='utf-8') as error_file:
        error_writer = csv.writer(error_file)
        error_writer.writerow(error_info)
        
'''without italic'''

def process_files_for_cik(cik):    
    read_folder = os.path.join(root_folder, cik)
    save_folder_factor = os.path.join(root_folder_risk_factors, cik)
    if not os.path.exists(save_folder_factor):
        os.makedirs(save_folder_factor)
    if read_folder == 'data/.DS_Store':
            return
    for file in os.listdir(read_folder):
        read_path = os.path.join(read_folder, file)
        if os.path.splitext(read_path)[1] == '.txt':
            read_folder = os.path.join(root_folder, cik)

            read_path = os.path.join(read_folder, file)
            content = []
            with open(read_path, 'r', encoding='utf-8') as input_txt:
                for line in input_txt:
                    row = line.strip().split()  # Assuming the file contains space-separated values
                    if not contains_only_numbers(row):
                        content.append(line)
            content = ''.join(content)
            content = re.sub(r'\s+', ' ', content)
            content = re.sub(r'\n.*\n<PAGE>', '', content)
            content = re.sub(r'\n.*\n</TEXT>', '', content)
            content = re.sub(r'<[^>]+>', '', content)
            # Find positions of 'ITEM 1A. RISK FACTORS' and 'Item 1B. Unresolved Staff Comments'
            pattern_1a = r'(?i)item\s+1a\.\s+risk\s+factors'
            pattern_1b = r'(?i)item\s+1b\.\s+unresolved\s+staff\s+comments'

            matches_1a = re.finditer(pattern_1a, content)
            positions_1a = [(match.start(),match.end()) for match in matches_1a]

            matches_1b = re.finditer(pattern_1b, content)
            positions_1b = [(match.start(),match.end()) for match in matches_1b]
        
            position, status = find_max_difference_pair(positions_1a,positions_1b)        
            if (position is not None) and (status is None):
                content = content [position[0]:position[1]]
                content = capital_sentence_detect(content)
                content = content.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')     
                if len(re.findall(r'\[heading\].*?\[/heading\]', content)) >= 4:
                    save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                    with open(save_path_factor, 'w+', encoding='utf-8') as f:
                        f.write(content)
                    f.close()
                else:
                    error_info = [cik, file]
                    with open(error_txt_csv_path, 'a', newline='', encoding='utf-8') as error_file:
                        error_writer = csv.writer(error_file)
                        error_writer.writerow(error_info)
            else:
                error_info = [cik, file]
                with open(error_txt_csv_path, 'a', newline='', encoding='utf-8') as error_file:
                    error_writer = csv.writer(error_file)
                    error_writer.writerow(error_info)
                
                
        if os.path.splitext(read_path)[1] == '.html':
            with open(read_path, 'r',encoding='utf-8') as f:
                content = f.read()
            f.close()
            content = content.replace('&#160;', ' ').replace('&nbsp;', ' ')
            pattern = r'Item 1A\.(\n|\s)*Risk Factors<\/[a-zA-Z]+>(<\/[a-zA-Z]+>)* <[a-zA-Z]+>\(Continued\)|ITEM 1A\.(.{0,10})RISK FACTORS (.{0,10})\(continued\)'
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
            soup = BeautifulSoup(content, 'html.parser')
            soup = find_page_break_tags(soup)
            soup = find_headings(soup)
            text_list = [text for text in soup.stripped_strings]
            if text_list[-1] != 'split_of_pages':
                text_list.append('split_of_pages')
            text_list = remove_page_footer(text_list)
            text_list = [text for text in text_list if text != 'split_of_pages']
            content = ' '.join(text_list)
            content = combine_adjacent_headings(content)
            
            pattern = r'\[heading\](.*?)\[/heading\]'
            content = re.sub(pattern, process_headings, content, flags=re.DOTALL)
            
            first_heading_position = find_heading_positions_with_keyword(content, ['item1a'])
            second_heading_position = find_heading_positions_with_keyword(content, ['item1b','item2','item3'])
            position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
            if (position is not None) and (status is None):
                result = content[position[0]:position[1]]
                result = re.sub(r'\s+', ' ', result)
                result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                result = remove_heading_risk_factors(result)
                result = remove_heading_risk_factors_continued(result)
                result = split_title_heading_content(result)
                result = filter_content_between_heading_pairs(result)
                result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                
                if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 10 headings
                    save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                    with open(save_path_factor, 'w+', encoding='utf-8') as f:
                        f.write(result)
                    f.close()
                else:
                    first_heading_position = find_heading_positions_with_keyword(content, ['Risk Factors'])
                    second_heading_position = find_heading_positions_with_keyword(content, ['Unresolved Staff Comments','Properties','Legal Proceedings'])
                    position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
                    if (position is not None) and (status is None):
                        result = content[position[0]:position[1]]
                        result = re.sub(r'\s+', ' ', result)
                        result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                        result = remove_heading_risk_factors(result)
                        result = remove_heading_risk_factors_continued(result)
                        result = split_title_heading_content(result)
                        result = filter_content_between_heading_pairs(result)
                        result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                        if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 10 headings
                            save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                            with open(save_path_factor, 'w+', encoding='utf-8') as f:
                                f.write(result)
                            f.close()
                        else:
                            save_error_info(cik, file, error_html_csv_path)
                    else:
                        save_error_info(cik, file, error_html_csv_path)
            else:
                first_heading_position = find_heading_positions_with_keyword(content, ['Risk Factors'])
                second_heading_position = find_heading_positions_with_keyword(content, ['Unresolved Staff Comments','Properties','Legal Proceedings'])
                position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
                if (position is not None) and (status is None):
                    result = content[position[0]:position[1]]
                    result = re.sub(r'\s+', ' ', result)
                    result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                    result = remove_heading_risk_factors(result)
                    result = remove_heading_risk_factors_continued(result)
                    result = split_title_heading_content(result)
                    result = filter_content_between_heading_pairs(result)
                    result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                    if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 10 headings
                        save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                        with open(save_path_factor, 'w+', encoding='utf-8') as f:
                            f.write(result)
                        f.close()
                    else:
                        save_error_info(cik, file, error_html_csv_path)
                else:
                    save_error_info(cik, file, error_html_csv_path)
                
                    
def process_files_for_cik_with_italic(cik, data_folder, save_folder, error_html_csv_path, error_txt_csv_path):    
    read_folder = os.path.join(data_folder, cik)
    save_folder_factor = os.path.join(save_folder, cik)
    if not os.path.exists(save_folder_factor):
        os.makedirs(save_folder_factor)
    if read_folder == f'{data_folder}/.DS_Store':
            return
    for file in os.listdir(read_folder):
        
        read_path = os.path.join(read_folder, file)
        
        # Skip it if the file alreay exists
        save_path_filing = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
        if os.path.exists(save_path_filing):
            continue
        
        
        #txt format reports
        if os.path.splitext(read_path)[1] == '.txt':
            content = []
            with open(read_path, 'r', encoding='utf-8') as input_txt:
                for line in input_txt:
                    row = line.strip().split()  # Assuming the file contains space-separated values
                    if not contains_only_numbers(row): #filter page numbers
                        content.append(line)
            
            #remove irrelevant tags
            content = ''.join(content)
            content = re.sub(r'\s+', ' ', content)
            content = re.sub(r'\n.*\n<PAGE>', '', content)
            content = re.sub(r'\n.*\n</TEXT>', '', content)
            content = re.sub(r'<[^>]+>', '', content)
            
            # Find positions of 'ITEM 1A. RISK FACTORS' and 'Item 1B. Unresolved Staff Comments'
            pattern_1a = r'(?i)item\s+1a\.\s+risk\s+factors'
            pattern_1b = r'(?i)item\s+1b\.\s+unresolved\s+staff\s+comments'

            matches_1a = re.finditer(pattern_1a, content)
            positions_1a = [(match.start(),match.end()) for match in matches_1a]

            matches_1b = re.finditer(pattern_1b, content)
            positions_1b = [(match.start(),match.end()) for match in matches_1b]
            
            #Get the position pairs that are more likely to be correct
            position, status = find_max_difference_pair(positions_1a,positions_1b)        
            if (position is not None) and (status is None):
                content = content [position[0]:position[1]]
                content = capital_sentence_detect(content) #find headings
                content = content.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')  #add \n for better view
                
                if len(re.findall(r'\[heading\].*?\[/heading\]', content)) >= 4: #check number of headings
                    save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                    with open(save_path_factor, 'w+', encoding='utf-8') as f:
                        f.write(content)
                    f.close()
                else:
                    #log error
                    error_info = [cik, file]
                    with open(error_txt_csv_path, 'a', newline='', encoding='utf-8') as error_file:
                        error_writer = csv.writer(error_file)
                        error_writer.writerow(error_info)
            else:
                #log error
                error_info = [cik, file]
                with open(error_txt_csv_path, 'a', newline='', encoding='utf-8') as error_file:
                    error_writer = csv.writer(error_file)
                    error_writer.writerow(error_info)
                
        
        #html format reports
        if os.path.splitext(read_path)[1] == '.html':
            with open(read_path, 'r',encoding='utf-8') as f:
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
            
            #get the position of keywords
            first_heading_position = find_heading_positions_with_keyword(content, ['item1a'])
            second_heading_position = find_heading_positions_with_keyword(content, ['item1b','item2','item3','STATEMENTS OF INCOME ANALYSIS'])
            position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
            
            if (position is not None) and (status is None):
                result = content[position[0]:position[1]] #get content by position
                result = re.sub(r'\s+', ' ', result) #remove nested \s
                result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                
                result = remove_heading_risk_factors(result)
                result = remove_heading_risk_factors_continued(result)
                
                result = split_title_heading_content(result)  #split title from headings
                
                result = filter_content_between_heading_pairs(result) #check the content between headings
                result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                result = remove_short_heading(result)
                
                #for better view
                result = re.sub(r'\s+', ' ', result)
                result = result.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')
                result = result.replace('[title]','\n\n[title]')
                
                if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 5 headings
                    save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                    with open(save_path_factor, 'w+', encoding='utf-8') as f:
                        f.write(result)
                    f.close()
                else:
                    #similar, different keywords
                    first_heading_position = find_heading_positions_with_keyword(content, ['Risk Factors'])
                    second_heading_position = find_heading_positions_with_keyword(content, ['Unresolved Staff Comments','Properties','Legal Proceedings','STATEMENTS OF INCOME ANALYSIS'])
                    position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
                    if (position is not None) and (status is None):
                        result = content[position[0]:position[1]]
                        result = re.sub(r'\s+', ' ', result)
                        result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                        result = remove_heading_risk_factors(result)
                        result = remove_heading_risk_factors_continued(result)
                        result = split_title_heading_content(result)
                        result = filter_content_between_heading_pairs(result)
                        result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                        result = remove_short_heading(result)
                        result = re.sub(r'\s+', ' ', result)
                        result = result.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')
                        result = result.replace('[title]','\n\n[title]')
                        if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 5 headings
                            save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                            with open(save_path_factor, 'w+', encoding='utf-8') as f:
                                f.write(result)
                            f.close()
                        else:
                            save_error_info(cik, file, error_html_csv_path)
                    else:
                        save_error_info(cik, file, error_html_csv_path)
            else:
                #similar, different keywords
                first_heading_position = find_heading_positions_with_keyword(content, ['Risk Factors'])
                second_heading_position = find_heading_positions_with_keyword(content, ['Unresolved Staff Comments','Properties','Legal Proceedings','STATEMENTS OF INCOME ANALYSIS'])
                position,status  = find_max_difference_pair(first_heading_position, second_heading_position)
                if (position is not None) and (status is None):
                    result = content[position[0]:position[1]]
                    result = re.sub(r'\s+', ' ', result)
                    result = result.replace('[heading]','\n[heading]').replace('[/heading]','[/heading]\n')
                    result = remove_heading_risk_factors(result)
                    result = remove_heading_risk_factors_continued(result)
                    result = split_title_heading_content(result)
                    result = filter_content_between_heading_pairs(result)
                    result = result.replace('[title]','\n[title]').replace('[/title]','[/title]\n')
                    result = remove_short_heading(result)
                    result = re.sub(r'\s+', ' ', result)
                    result = result.replace('[heading]','\n\n[heading]').replace('[/heading]','[/heading]\n\n')
                    result = result.replace('[title]','\n\n[title]')
                    if len(re.findall(r'\[heading\].*?\[/heading\]', result)) >= 5:  # Check if there are at least 10 headings
                        save_path_factor = os.path.join(save_folder_factor, os.path.splitext(file)[0] + '.txt')
                        with open(save_path_factor, 'w+', encoding='utf-8') as f:
                            f.write(result)
                        f.close()
                    else:
                        save_error_info(cik, file, error_html_csv_path)
                else:
                    save_error_info(cik, file, error_html_csv_path)
                
def executor(data_folder, save_folder):
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for cik in os.listdir(data_folder):
            print('Processing CIK_executing risk factor process:', cik)
            future = executor.submit(process_files_for_cik_with_italic, cik, data_folder, save_folder)
            futures.append(future)
        
        # Wait for all tasks to complete
        for future in futures:
            future.result()
        
        # All tasks are completed, shutdown the executor
        executor.shutdown() 
                    

# root_folder = '/Users/apple/PROJECT/Code_4_10k/top10_data'
# root_folder_risk_factors = '/Users/apple/PROJECT/Code_4_10k/top10_risk_factors'


# #concurrent run, take several hours
# with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
#     futures = []
#     for cik in os.listdir(root_folder):
#         print('Processing CIK_executing risk factor process:', cik)
#         future = executor.submit(process_files_for_cik_with_italic, cik)
#         futures.append(future)
    
#     # Wait for all tasks to complete
#     for future in futures:
#         future.result()
    
#     # All tasks are completed, shutdown the executor
#     executor.shutdown()