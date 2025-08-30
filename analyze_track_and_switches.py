import pandas as pd
import re
import os
import glob




def find_header_row(df, keywords=("序号", "进路类型", "道岔", "轨道区段")):
    """动态查找包含关键字的表头行"""
    for idx, row in df.iterrows():
        if any(str(cell).strip() in keywords for cell in row):
            return idx
    raise ValueError("未找到有效表头行，请检查文件格式")

def parse_track_info(entry):
    """解析轨道区段信息，返回长度和区段名称"""
    if not isinstance(entry, str):
        return None, None
    parts = re.split(r'[\\,]', entry.strip())
    if len(parts) < 4:
        return None, None
    try:
        return int(parts[0]), parts[-1].strip()
    except ValueError:
        return None, None

input_dir = os.getcwd()
output_base_dir = os.getcwd()

# Find all relevant Excel files
excel_files = glob.glob(os.path.join(input_dir, '*进路信息表*.xlsx')) + \
              glob.glob(os.path.join(input_dir, '*进路信息表*.xls'))

if not excel_files:
    pass
else:
    for file_path in excel_files:
        try:
            xls = pd.ExcelFile(file_path)
            df_raw = xls.parse(xls.sheet_names[0], header=None) # Disable auto header detection
            
            header_row_idx = find_header_row(df_raw)
            columns = df_raw.iloc[header_row_idx].tolist()

            # Dynamic column mapping
            col_mapping = {
                "道岔": [col for col in columns if "道岔" in str(col)],
                "轨道区段": [col for col in columns if "轨道区段" in str(col)],
                "进路类型": [col for col in columns if "进路类型" in str(col)]
            }

            for key in col_mapping:
                if not col_mapping[key]:
                    raise ValueError(f"文件 {os.path.basename(file_path)} 缺失必要列: {key}")

            switch_col_name = col_mapping["道岔"][0]
            track_col_name = col_mapping["轨道区段"][0]
            route_type_col_name = col_mapping["进路类型"][0]

            # Extract data using identified column names
            df_data = df_raw.iloc[header_row_idx + 1:].copy()
            df_data.columns = columns # Assign full header to the data DataFrame
            
            # Select only the relevant columns for processing
            data_for_processing = df_data[[switch_col_name, track_col_name, route_type_col_name]].copy()
            data_for_processing.columns = ['道岔组合_原始', '轨道区段信息_原始', '进路类型_原始']

            track_pattern = re.compile(r'(\d+)(?:-(\d+))?DG')
            

            results = []

            for _, row in data_for_processing.iterrows():
                daocha_str_raw = str(row['道岔组合_原始']).strip()
                track_info_raw = str(row['轨道区段信息_原始']).strip().replace('<br>', ',')

                

                found_raw_switches = [s.strip() for s in daocha_str_raw.split(',') if s.strip()]

                track_entries = [entry.strip() for entry in re.split(r'[,，\n]', track_info_raw) if entry.strip()]

                if not track_entries: # If no valid track entries found for this row
                    current_route_type = str(df_data.iloc[_, df_data.columns.get_loc(route_type_col_name)]).strip()
                    remark = ''
                    processed_switches = found_raw_switches
                    if '发车' in current_route_type:
                        processed_switches.reverse()
                        remark = '道岔组合逆序'
                    results.append({
                        '轨道区段名称': '', # Default for empty track section
                        '道岔组合': ','.join(processed_switches) if processed_switches else '无',
                        '区段长度': 0, # Default length for empty track section
                        '备注': remark
                    })
                else:
                    for entry in track_entries:
                        length, section_name = parse_track_info(entry)
                        if not section_name or not length:
                            continue
                        
                        # Process switch combinations for the current track section
                        matched_switches = []

                        # Extract numbers from the current track section (e.g., 201 from 201DG, or 201, 202, 203 from 201-203DG)
                        track_match = track_pattern.match(section_name)
                        relevant_track_nums = set()

                        if track_match: # Only proceed with switch matching if it's a DG section
                            start_num = int(track_match.group(1))
                            end_num = int(track_match.group(2)) if track_match.group(2) else start_num
                            relevant_track_nums.update(map(str, range(start_num, end_num + 1)))

                            for raw_switch_part in found_raw_switches:
                                # Check if any number from the track section appears as a whole number within the switch part
                                is_relevant_switch = False
                                for s_num_str in relevant_track_nums:
                                    # Use word boundaries to ensure whole number match (e.g., 201 matches 201, not 20 in 201)
                                    if re.search(r'\b' + re.escape(s_num_str) + r'\b', raw_switch_part):
                                        is_relevant_switch = True
                                        break

                                if is_relevant_switch:
                                    matched_switches.append(raw_switch_part) # Directly append raw_switch_part

                        current_route_type = str(row['进路类型_原始']).strip()
                        remark = ''
                        if '发车' in current_route_type:
                            matched_switches.reverse()
                            remark = '道岔组合逆序'

                        results.append({
                            '轨道区段名称': section_name,
                            '道岔组合': ','.join(matched_switches) if matched_switches else '无', # Use matched_switches directly
                            '区段长度': length,
                            '备注': remark
                        })

            result_df = pd.DataFrame(results).reset_index(drop=True)

            # --- 正线标记 ---
            df_full_for_mainline = df_data[[route_type_col_name, track_col_name, switch_col_name]].copy()
            df_full_for_mainline.columns = ['进路类型', '轨道区段', '道岔']

            def is_mainline_section(track_name, daocha_combo):
                if not daocha_combo or daocha_combo == '无':
                    return False
                
                current_daocha_normalized = [d.strip() for d in daocha_combo.split(',') if d.strip()]
                
                for _, row_full in df_full_for_mainline.iterrows():
                    jl_type = str(row_full['进路类型']).strip()
                    if jl_type not in ["正线接车", "反向正线接车"]:
                        continue
                    
                    jl_tracks = [t.split('\\')[-1].strip() for t in re.split(r'[,，\n]', str(row_full['轨道区段'])) if t.strip()]
                    
                    # Normalize and clean the switches from the full data for comparison
                    jl_daocha_raw = str(row_full['道岔']).strip()
                    jl_daocha_normalized = [s.strip() for s in jl_daocha_raw.split(',') if s.strip()]
                    
                    if track_name in jl_tracks and all(d in jl_daocha_normalized for d in current_daocha_normalized):
                        return True
                return False

            result_df['正线标记'] = result_df.apply(
                lambda row: '正线区段' if is_mainline_section(row['轨道区段名称'], row['道岔组合']) else '',
                axis=1
            )

            # --- Output ---
            output_filename = os.path.splitext(os.path.basename(file_path))[0] + '_分析结果.xlsx'
            output_excel = os.path.join(output_base_dir, output_filename)
            result_df.to_excel(output_excel, index=False)

        except Exception as e:
            pass