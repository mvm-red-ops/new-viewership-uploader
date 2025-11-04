import streamlit as st
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional
import re
from difflib import SequenceMatcher
import boto3

from snowflake_utils import SnowflakeConnection
from column_mapper import ColumnMapper
from config import load_aws_config, get_environment_name, get_config
from transformations import apply_transformation, TRANSFORMATION_TEMPLATES, preview_transformation_step
from wide_format_handler import detect_and_transform

# Transformation Builder Modal
@st.dialog("🔧 Transformation Builder", width="large")
def transformation_builder_modal(column_name, sample_data, full_data=None):
    """Modal dialog for building transformations

    Args:
        column_name: Name of the column being transformed
        sample_data: Small sample (e.g., 10 rows) for fast preview
        full_data: Full column data for profiling (optional)
    """

    # Store that this modal is open
    st.session_state[f"modal_open_{column_name}"] = True

    # Store full data for profiling
    if full_data is not None:
        st.session_state[f"full_data_for_profile_{column_name}"] = full_data

    st.write(f"**Transforming column:** `{column_name}`")

    # Preview controls
    col1, col2 = st.columns([3, 1])
    with col1:
        with st.expander("📊 Sample Data (first 5 rows)", expanded=False):
            for i, val in enumerate(sample_data[:5]):
                st.caption(f"Row {i+1}: `{val}`")
    with col2:
        preview_count = st.number_input(
            "Preview samples",
            min_value=3,
            max_value=min(50, len(sample_data)),
            value=5,
            step=1,
            key=f"preview_count_{column_name}",
            help="How many samples to show in previews"
        )

    st.divider()

    # Initialize approach in session state if not exists
    approach_key = f"modal_approach_{column_name}"
    if approach_key not in st.session_state:
        st.session_state[approach_key] = "Build Custom Steps"

    # Choose approach (don't set index since we're using session state key)
    approach = st.radio(
        "Choose your approach:",
        options=["Use Template", "Build Custom Steps"],
        horizontal=True,
        key=approach_key
    )

    transformation_config = None
    steps = []  # Initialize steps for all approaches

    if approach == "Use Template":
        st.write("### Select a Template")

        # Show templates with descriptions
        template_options = ["None"] + list(TRANSFORMATION_TEMPLATES.keys())
        transform_type = st.selectbox(
            "Template",
            options=template_options,
            key=f"modal_template_{column_name}",
            help="Choose a pre-built transformation"
        )

        if transform_type != "None":
            transformation_config = TRANSFORMATION_TEMPLATES[transform_type]

            # Show preview
            st.write("### Preview")
            try:
                import pandas as pd
                sample_series = pd.Series(sample_data[:preview_count])
                transformed = apply_transformation(sample_series, transformation_config)

                col1, col2 = st.columns(2)
                with col1:
                    st.caption("**Before:**")
                    for i, val in enumerate(sample_series.values):
                        st.caption(f"  {i+1}. `{val}`")
                with col2:
                    st.caption("**After:**")
                    for i, val in enumerate(transformed.values):
                        # Handle lists specially
                        if isinstance(val, list):
                            st.caption(f"  {i+1}. Split into {len(val)} parts:")
                            for idx, part in enumerate(val):
                                st.caption(f"      [{idx}] `{part}`")
                        else:
                            st.caption(f"  {i+1}. `{val}`")
            except Exception as e:
                st.error(f"Preview error: {str(e)}")

    elif approach == "Build Custom Steps":
        st.write("### Build Transformation Steps")
        st.caption("Add steps to transform your data. Each step shows the result.")

        # Initialize steps in session state
        step_key = f"modal_steps_{column_name}"
        if step_key not in st.session_state:
            st.session_state[step_key] = []

        steps = st.session_state[step_key]

        # Display each step
        if steps:
            st.write("#### Steps")

            for step_idx, step in enumerate(steps):
                with st.container(border=True):
                    col_header, col_delete = st.columns([5, 1])
                    with col_header:
                        st.write(f"**Step {step_idx + 1}**")
                    with col_delete:
                        if st.button("🗑️", key=f"modal_delete_{column_name}_{step_idx}", help="Delete this step"):
                            steps.pop(step_idx)
                            st.rerun()

                    col1, col2 = st.columns([1, 2])

                    with col1:
                        # Check if previous step resulted in an array/split
                        prev_is_array = False
                        if step_idx > 0:
                            try:
                                test_val = sample_data[0]
                                for i in range(step_idx):
                                    test_val = preview_transformation_step(test_val, steps[i])
                                prev_is_array = isinstance(test_val, list)
                            except:
                                pass

                        step_type = st.selectbox(
                            "Operation",
                            options=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)", "Clean Number", "Parse Time", "IF Condition"],
                            key=f"modal_step_type_{column_name}_{step_idx}",
                            index=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)", "Clean Number", "Parse Time", "IF Condition"].index(step.get('ui_type', 'Split')) if step.get('ui_type') in ["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)", "Clean Number", "Parse Time", "IF Condition"] else 0
                        )

                        # Show helpful hint if previous step was a split
                        if prev_is_array and step_type not in ["Extract Part"]:
                            st.info("💡 Previous step created an array. Use **Extract Part** to select which element you want!")

                    with col2:
                        # Show different inputs based on operation type
                        if step_type == "Split":
                            # Use text_input for single character delimiters (most common)
                            delimiter = st.text_input(
                                "Delimiter (character(s) to split on)",
                                value=step.get('params', {}).get('delimiter', ','),
                                key=f"modal_delimiter_{column_name}_{step_idx}",
                                placeholder="e.g., - or , or |",
                                help="Enter the character(s) to split on. Common: comma (,), hyphen (-), pipe (|), underscore (_)"
                            )
                            step['type'] = 'split'
                            step['params'] = {'delimiter': delimiter}
                            step['ui_type'] = 'Split'

                            # Show what delimiter actually is
                            if delimiter:
                                delimiter_display = delimiter.replace(' ', '␣')  # Show spaces clearly
                                st.caption(f"Will split on: `{delimiter_display}` ({len(delimiter)} char{'s' if len(delimiter) != 1 else ''})")
                            else:
                                st.warning("⚠️ Delimiter is empty!")

                        elif step_type == "Extract Part":
                            index = st.number_input(
                                "Index (which part to extract, 0-based)",
                                value=step.get('params', {}).get('index', 0),
                                min_value=0,
                                key=f"modal_index_{column_name}_{step_idx}"
                            )
                            step['type'] = 'extract_index'
                            step['params'] = {'index': index}
                            step['ui_type'] = 'Extract Part'

                        elif step_type == "Remove Prefix":
                            prefix = st.text_input(
                                "Prefix to remove",
                                value=step.get('params', {}).get('prefix', ''),
                                key=f"modal_prefix_{column_name}_{step_idx}",
                                placeholder="e.g. 'Channel: '"
                            )
                            step['type'] = 'remove_prefix'
                            step['params'] = {'prefix': prefix}
                            step['ui_type'] = 'Remove Prefix'

                        elif step_type == "Trim (All Spaces)":
                            step['type'] = 'trim_all'
                            step['params'] = {}
                            step['ui_type'] = 'Trim (All Spaces)'
                            st.caption("Removes all spaces from text")

                        elif step_type == "Trim (Ends Only)":
                            step['type'] = 'trim_ends'
                            step['params'] = {}
                            step['ui_type'] = 'Trim (Ends Only)'
                            st.caption("Removes leading/trailing spaces")

                        elif step_type == "Clean Number":
                            step['type'] = 'clean_numeric'
                            step['params'] = {}
                            step['ui_type'] = 'Clean Number'
                            st.caption("Removes $, %, commas")

                        elif step_type == "Parse Time":
                            unit = st.selectbox(
                                "Output unit",
                                options=["hours", "minutes"],
                                key=f"modal_time_unit_{column_name}_{step_idx}"
                            )
                            step['type'] = 'parse_time'
                            step['params'] = {'output_unit': unit}
                            step['ui_type'] = 'Parse Time'

                        elif step_type == "IF Condition":
                            st.write("**Conditional Logic:**")

                            # Condition type and value
                            cond_col1, cond_col2 = st.columns([1, 2])
                            with cond_col1:
                                cond_type = st.selectbox(
                                    "Condition",
                                    options=["Contains", "Starts With", "Ends With", "Equals", "Regex Match"],
                                    key=f"modal_if_cond_type_{column_name}_{step_idx}",
                                    index=["Contains", "Starts With", "Ends With", "Equals", "Regex Match"].index(step.get('condition', {}).get('ui_type', 'Contains')) if step.get('condition', {}).get('ui_type') in ["Contains", "Starts With", "Ends With", "Equals", "Regex Match"] else 0
                                )
                            with cond_col2:
                                check_value = st.text_input(
                                    "Value to check",
                                    value=step.get('condition', {}).get('value', ''),
                                    key=f"modal_if_cond_value_{column_name}_{step_idx}",
                                    placeholder='e.g., "Channel:" or "_"'
                                )

                            # Map UI type to internal type
                            type_map = {
                                "Contains": "contains",
                                "Starts With": "starts_with",
                                "Ends With": "ends_with",
                                "Equals": "equals",
                                "Regex Match": "regex_match"
                            }

                            if 'condition' not in step:
                                step['condition'] = {}
                            step['condition']['type'] = type_map[cond_type]
                            step['condition']['value'] = check_value
                            step['condition']['ui_type'] = cond_type
                            step['type'] = 'conditional'
                            step['ui_type'] = 'IF Condition'

                            # Initialize conditions list if needed
                            if 'conditions' not in step:
                                step['conditions'] = [{
                                    'type': step['condition']['type'],
                                    'value': step['condition']['value'],
                                    'steps': []
                                }]
                            else:
                                # Update the first condition with current values
                                step['conditions'][0]['type'] = step['condition']['type']
                                step['conditions'][0]['value'] = step['condition']['value']

                            # Show match count and samples
                            if check_value:
                                matching_samples = []
                                non_matching_samples = []

                                for val in sample_data:
                                    val_str = str(val)
                                    matched = False

                                    if step['condition']['type'] == 'contains':
                                        matched = check_value in val_str
                                    elif step['condition']['type'] == 'starts_with':
                                        matched = val_str.startswith(check_value)
                                    elif step['condition']['type'] == 'ends_with':
                                        matched = val_str.endswith(check_value)
                                    elif step['condition']['type'] == 'equals':
                                        matched = val_str == check_value
                                    elif step['condition']['type'] == 'regex_match':
                                        import re
                                        matched = bool(re.search(check_value, val_str))

                                    if matched:
                                        matching_samples.append(val)
                                    else:
                                        non_matching_samples.append(val)

                                match_col1, match_col2 = st.columns(2)
                                with match_col1:
                                    st.info(f"✓ **{len(matching_samples)}** match")
                                    if matching_samples:
                                        with st.expander(f"Preview ({min(preview_count, len(matching_samples))} samples)"):
                                            for i, sample in enumerate(matching_samples[:preview_count]):
                                                st.caption(f"{i+1}. `{sample}`")
                                with match_col2:
                                    st.warning(f"✗ **{len(non_matching_samples)}** don't match")
                                    if non_matching_samples:
                                        with st.expander(f"Preview ({min(preview_count, len(non_matching_samples))} samples)"):
                                            for i, sample in enumerate(non_matching_samples[:preview_count]):
                                                st.caption(f"{i+1}. `{sample}`")

                            # THEN steps section
                            st.write("**THEN** (for matching rows):")

                            if 'steps' not in step['conditions'][0]:
                                step['conditions'][0]['steps'] = []

                            then_steps = step['conditions'][0]['steps']

                            for then_idx, then_step in enumerate(then_steps):
                                with st.container(border=True):
                                    then_col1, then_col2 = st.columns([5, 1])
                                    with then_col1:
                                        st.caption(f"**Step {then_idx + 1}**")
                                    with then_col2:
                                        if st.button("×", key=f"del_then_{column_name}_{step_idx}_{then_idx}"):
                                            then_steps.pop(then_idx)
                                            st.rerun()

                                    # Edit the step inline
                                    then_step_type = st.selectbox(
                                        "Operation",
                                        options=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"],
                                        key=f"then_type_{column_name}_{step_idx}_{then_idx}",
                                        index=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"].index(then_step.get('ui_type', 'Split')) if then_step.get('ui_type') in ["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"] else 0
                                    )

                                    # Configure based on type
                                    if then_step_type == "Split":
                                        delimiter = st.text_input(
                                            "Delimiter",
                                            value=then_step.get('params', {}).get('delimiter', '  '),
                                            key=f"then_delim_{column_name}_{step_idx}_{then_idx}"
                                        )
                                        then_step['type'] = 'split'
                                        then_step['params'] = {'delimiter': delimiter}
                                        then_step['ui_type'] = 'Split'
                                    elif then_step_type == "Extract Part":
                                        index = st.number_input(
                                            "Index (0-based)",
                                            min_value=0,
                                            value=then_step.get('params', {}).get('index', 0),
                                            key=f"then_idx_{column_name}_{step_idx}_{then_idx}"
                                        )
                                        then_step['type'] = 'extract_index'
                                        then_step['params'] = {'index': index}
                                        then_step['ui_type'] = 'Extract Part'
                                    elif then_step_type == "Remove Prefix":
                                        prefix = st.text_input(
                                            "Prefix to remove",
                                            value=then_step.get('params', {}).get('prefix', ''),
                                            key=f"then_prefix_{column_name}_{step_idx}_{then_idx}"
                                        )
                                        then_step['type'] = 'remove_prefix'
                                        then_step['params'] = {'prefix': prefix}
                                        then_step['ui_type'] = 'Remove Prefix'
                                    elif then_step_type == "Trim (All Spaces)":
                                        then_step['type'] = 'trim_all'
                                        then_step['params'] = {}
                                        then_step['ui_type'] = 'Trim (All Spaces)'
                                    elif then_step_type == "Trim (Ends Only)":
                                        then_step['type'] = 'trim_ends'
                                        then_step['params'] = {}
                                        then_step['ui_type'] = 'Trim (Ends Only)'

                                    # Show preview after this THEN step
                                    st.write("**Result:**")
                                    try:
                                        # Find a matching sample
                                        condition = step.get('condition', {})
                                        check_value = condition.get('value', '')
                                        matching_sample = None

                                        for val in sample_data:
                                            val_str = str(val)
                                            matched = False

                                            if condition.get('type') == 'contains':
                                                matched = check_value in val_str
                                            elif condition.get('type') == 'starts_with':
                                                matched = val_str.startswith(check_value)
                                            elif condition.get('type') == 'ends_with':
                                                matched = val_str.endswith(check_value)
                                            elif condition.get('type') == 'equals':
                                                matched = val_str == check_value

                                            if matched:
                                                matching_sample = val
                                                break

                                        if matching_sample:
                                            # Apply all THEN steps up to this one
                                            result = matching_sample
                                            for i in range(then_idx + 1):
                                                result = preview_transformation_step(result, then_steps[i])

                                            if isinstance(result, list):
                                                st.info(f"📋 Split into {len(result)} parts: `{result}`")
                                            else:
                                                st.success(f"✓ `{result}`")
                                        else:
                                            st.caption("(No matching samples)")
                                    except Exception as e:
                                        st.error(f"Error: {str(e)}")

                            if st.button("➕ Add THEN step", key=f"add_then_{column_name}_{step_idx}"):
                                then_steps.append({'type': 'split', 'params': {'delimiter': '  '}, 'ui_type': 'Split'})
                                st.rerun()

                            # ELSE toggle and steps
                            st.divider()

                            if 'else_steps' not in step:
                                step['else_steps'] = []

                            use_else = st.checkbox(
                                "Add ELSE branch (for non-matching rows)",
                                value=len(step['else_steps']) > 0,
                                key=f"use_else_{column_name}_{step_idx}"
                            )

                            if use_else:
                                st.write("**ELSE** (for non-matching rows):")

                                for else_idx, else_step in enumerate(step['else_steps']):
                                    with st.container(border=True):
                                        else_col1, else_col2 = st.columns([5, 1])
                                        with else_col1:
                                            st.caption(f"**Step {else_idx + 1}**")
                                        with else_col2:
                                            if st.button("×", key=f"del_else_{column_name}_{step_idx}_{else_idx}"):
                                                step['else_steps'].pop(else_idx)
                                                st.rerun()

                                        # Edit the step inline
                                        else_step_type = st.selectbox(
                                            "Operation",
                                            options=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"],
                                            key=f"else_type_{column_name}_{step_idx}_{else_idx}",
                                            index=["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"].index(else_step.get('ui_type', 'Split')) if else_step.get('ui_type') in ["Split", "Extract Part", "Remove Prefix", "Trim (All Spaces)", "Trim (Ends Only)"] else 0
                                        )

                                        # Configure based on type
                                        if else_step_type == "Split":
                                            delimiter = st.text_input(
                                                "Delimiter",
                                                value=else_step.get('params', {}).get('delimiter', '_'),
                                                key=f"else_delim_{column_name}_{step_idx}_{else_idx}"
                                            )
                                            else_step['type'] = 'split'
                                            else_step['params'] = {'delimiter': delimiter}
                                            else_step['ui_type'] = 'Split'
                                        elif else_step_type == "Extract Part":
                                            index = st.number_input(
                                                "Index (0-based)",
                                                min_value=0,
                                                value=else_step.get('params', {}).get('index', 0),
                                                key=f"else_idx_{column_name}_{step_idx}_{else_idx}"
                                            )
                                            else_step['type'] = 'extract_index'
                                            else_step['params'] = {'index': index}
                                            else_step['ui_type'] = 'Extract Part'
                                        elif else_step_type == "Remove Prefix":
                                            prefix = st.text_input(
                                                "Prefix to remove",
                                                value=else_step.get('params', {}).get('prefix', ''),
                                                key=f"else_prefix_{column_name}_{step_idx}_{else_idx}"
                                            )
                                            else_step['type'] = 'remove_prefix'
                                            else_step['params'] = {'prefix': prefix}
                                            else_step['ui_type'] = 'Remove Prefix'
                                        elif else_step_type == "Trim (All Spaces)":
                                            else_step['type'] = 'trim_all'
                                            else_step['params'] = {}
                                            else_step['ui_type'] = 'Trim (All Spaces)'
                                        elif else_step_type == "Trim (Ends Only)":
                                            else_step['type'] = 'trim_ends'
                                            else_step['params'] = {}
                                            else_step['ui_type'] = 'Trim (Ends Only)'

                                        # Show preview after this ELSE step
                                        st.write("**Result:**")
                                        try:
                                            # Find a non-matching sample
                                            condition = step.get('condition', {})
                                            check_value = condition.get('value', '')
                                            non_matching_sample = None

                                            for val in sample_data:
                                                val_str = str(val)
                                                matched = False

                                                if condition.get('type') == 'contains':
                                                    matched = check_value in val_str
                                                elif condition.get('type') == 'starts_with':
                                                    matched = val_str.startswith(check_value)
                                                elif condition.get('type') == 'ends_with':
                                                    matched = val_str.endswith(check_value)
                                                elif condition.get('type') == 'equals':
                                                    matched = val_str == check_value

                                                if not matched:
                                                    non_matching_sample = val
                                                    break

                                            if non_matching_sample:
                                                # Apply all ELSE steps up to this one
                                                result = non_matching_sample
                                                for i in range(else_idx + 1):
                                                    result = preview_transformation_step(result, step['else_steps'][i])

                                                if isinstance(result, list):
                                                    st.info(f"📋 Split into {len(result)} parts: `{result}`")
                                                else:
                                                    st.success(f"✓ `{result}`")
                                            else:
                                                st.caption("(No non-matching samples)")
                                        except Exception as e:
                                            st.error(f"Error: {str(e)}")

                                if st.button("➕ Add ELSE step", key=f"add_else_{column_name}_{step_idx}"):
                                    step['else_steps'].append({'type': 'split', 'params': {'delimiter': '_'}, 'ui_type': 'Split'})
                                    st.rerun()
                            elif step['else_steps']:
                                # User unchecked the box, clear else steps
                                step['else_steps'] = []

                    # Show preview after this step
                    st.write("**Result after this step:**")
                    try:
                        # For conditional steps, show different previews for matching and non-matching
                        if step.get('type') == 'conditional':
                            # Show preview for matching rows
                            matching_preview = []
                            non_matching_preview = []

                            for val in sample_data[:preview_count]:
                                val_str = str(val)
                                matched = False

                                condition = step.get('condition', {})
                                check_value = condition.get('value', '')

                                if condition.get('type') == 'contains':
                                    matched = check_value in val_str
                                elif condition.get('type') == 'starts_with':
                                    matched = val_str.startswith(check_value)
                                elif condition.get('type') == 'ends_with':
                                    matched = val_str.endswith(check_value)
                                elif condition.get('type') == 'equals':
                                    matched = val_str == check_value
                                elif condition.get('type') == 'regex_match':
                                    import re
                                    matched = bool(re.search(check_value, val_str))

                                # Apply previous steps first
                                preview_value = val
                                for i in range(step_idx):
                                    preview_value = preview_transformation_step(preview_value, steps[i])

                                # Now apply this conditional step
                                result = preview_transformation_step(preview_value, step)

                                if matched:
                                    matching_preview.append((val, result))
                                else:
                                    non_matching_preview.append((val, result))

                            if matching_preview:
                                st.success("✓ **Matching rows** (THEN branch):")
                                for orig, result in matching_preview[:2]:
                                    if isinstance(result, list):
                                        st.caption(f"  `{str(orig)[:40]}...` → Split into {len(result)} parts: `{result}`")
                                    else:
                                        st.caption(f"  `{str(orig)[:40]}...` → `{result}`")

                            if non_matching_preview and step.get('else_steps'):
                                st.warning("✗ **Non-matching rows** (ELSE branch):")
                                for orig, result in non_matching_preview[:2]:
                                    if isinstance(result, list):
                                        st.caption(f"  `{str(orig)[:40]}...` → Split into {len(result)} parts: `{result}`")
                                    else:
                                        st.caption(f"  `{str(orig)[:40]}...` → `{result}`")
                        else:
                            # Regular step preview - show multiple samples
                            col_before, col_after = st.columns(2)

                            with col_before:
                                st.caption("**Before this step:**")
                            with col_after:
                                st.caption("**After this step:**")

                            # Show preview for first 3 samples
                            for sample_idx in range(min(3, len(sample_data))):
                                preview_value = sample_data[sample_idx]

                                # Apply all steps up to current one
                                for i in range(step_idx + 1):
                                    preview_value = preview_transformation_step(preview_value, steps[i])

                                with col_before:
                                    # Show value before this step
                                    before_value = sample_data[sample_idx]
                                    for i in range(step_idx):
                                        before_value = preview_transformation_step(before_value, steps[i])
                                    st.caption(f"  {sample_idx+1}. `{before_value}`")

                                with col_after:
                                    if isinstance(preview_value, list):
                                        st.caption(f"  {sample_idx+1}. 📋 Split → {len(preview_value)} parts")
                                        for idx, part in enumerate(preview_value[:3]):  # Show first 3 parts
                                            st.caption(f"      [{idx}] `{part}`")
                                        if len(preview_value) > 3:
                                            st.caption(f"      ... and {len(preview_value) - 3} more")
                                    else:
                                        st.caption(f"  {sample_idx+1}. `{preview_value}`")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

        # Button to add new step
        if st.button("➕ Add Step", key=f"modal_add_step_{column_name}", use_container_width=True):
            steps.append({'type': 'split', 'params': {'delimiter': ','}, 'ui_type': 'Split'})
            st.rerun()

        # Show final result for all samples
        if steps:
            st.divider()
            st.write("### Final Result Preview")
            transformation_config = {'type': 'chain', 'steps': steps}

            try:
                import pandas as pd
                sample_series = pd.Series(sample_data[:preview_count])
                transformed = apply_transformation(sample_series, transformation_config)

                col1, col2 = st.columns(2)
                with col1:
                    st.caption("**Before:**")
                    for i, val in enumerate(sample_series.values):
                        st.caption(f"  {i+1}. `{val}`")
                with col2:
                    st.caption("**After:**")
                    for i, val in enumerate(transformed.values):
                        # Handle lists specially
                        if isinstance(val, list):
                            st.caption(f"  {i+1}. Split into {len(val)} parts:")
                            for idx, part in enumerate(val):
                                st.caption(f"      [{idx}] `{part}`")
                        else:
                            st.caption(f"  {i+1}. `{val}`")
            except Exception as e:
                st.error(f"Transformation error: {str(e)}")

    # Action buttons
    st.divider()

    # Profile Data button (only for custom steps, not templates)
    if steps and transformation_config and approach == "Build Custom Steps":
        if st.button("🔍 Profile Data", use_container_width=True, help="Analyze transformation across all data to detect issues"):
            # Close transformation builder modal
            st.session_state[f"modal_open_{column_name}"] = False
            # Open profile modal
            st.session_state[f"profile_modal_open_{column_name}"] = True
            st.session_state[f"profile_transformation_{column_name}"] = transformation_config
            # Store the steps for the profile modal
            st.session_state[f"profile_steps_{column_name}"] = steps
            st.rerun()

    col1, col2 = st.columns(2)

    with col1:
        if st.button("✅ Apply Transformation", type="primary", use_container_width=True):
            # Store the transformation config in session state
            st.session_state[f"transform_config_{column_name}"] = transformation_config
            st.session_state[f"transform_applied_{column_name}"] = True
            st.session_state[f"modal_open_{column_name}"] = False
            st.rerun()

    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state[f"transform_applied_{column_name}"] = False
            st.session_state[f"modal_open_{column_name}"] = False
            st.rerun()


# Profile Data Modal
@st.dialog("🔍 Data Profile", width="large")
def profile_data_modal(column_name, transformation_config, full_data, steps):
    """Modal for profiling transformation across full dataset

    Args:
        column_name: Name of the column
        transformation_config: The transformation to analyze
        full_data: Full column data to analyze
        steps: Transformation steps
    """

    st.write(f"**Analyzing transformation for:** `{column_name}`")
    st.caption(f"Analyzing {len(full_data):,} records...")

    try:
        import pandas as pd
        from collections import Counter

        # First: Quick pattern analysis on FULL dataset (grouped by unique values)
        st.subheader("📊 Pattern Analysis (Full Dataset)")

        # Find delimiter from first split step
        delimiter = None
        for step in steps:
            if step.get('type') == 'split':
                delimiter = step.get('params', {}).get('delimiter', ',')
                break

        if delimiter:
            # Group by unique values and count delimiter occurrences
            unique_values = list(set(full_data))
            delimiter_counts = {}

            for val in unique_values:
                count = str(val).count(delimiter)
                if count not in delimiter_counts:
                    delimiter_counts[count] = {'count': 0, 'examples': []}
                delimiter_counts[count]['count'] += full_data.count(val)
                if len(delimiter_counts[count]['examples']) < 3:
                    delimiter_counts[count]['examples'].append(val)

            # Show distribution
            st.write(f"**Delimiter (`{delimiter}`) distribution across {len(full_data):,} records:**")
            total_records = len(full_data)

            for num_delimiters in sorted(delimiter_counts.keys()):
                info = delimiter_counts[num_delimiters]
                count = info['count']
                percentage = (count / total_records) * 100
                num_parts = num_delimiters + 1
                emoji = "✓" if percentage > 80 else "⚠️" if percentage > 5 else "ℹ️"

                st.caption(f"  {emoji} **{count:,} records ({percentage:.1f}%)** have {num_delimiters} delimiter(s) → **{num_parts} parts** after split")

                # Show examples
                if info['examples']:
                    examples_str = ', '.join(f"`{ex}`" for ex in info['examples'][:3])
                    st.caption(f"     Examples: {examples_str}")

            # Suggest extraction strategy for territory/second value
            if len(delimiter_counts) > 1:
                st.info("💡 **Tip for extracting second value (e.g., territory):** If you want to extract the second part regardless of total parts, use **Extract Part** with index=1. This works for both 2-part (`Partner-us`) and 3-part (`Partner-us-mobile`) splits.")

            st.divider()

        # Then: Detailed transformation analysis on sample
        st.subheader("🔬 Transformation Result Analysis (Sample)")

        # Limit to reasonable size for transformation analysis
        analysis_size = min(1000, len(full_data))
        analysis_data = full_data[:analysis_size]

        if analysis_size < len(full_data):
            st.caption(f"Analyzing transformation on first {analysis_size:,} of {len(full_data):,} records")

        # Apply transformation to analysis data
        full_sample_series = pd.Series(analysis_data)
        full_transformed = apply_transformation(full_sample_series, transformation_config)

        # Check if any step was a split
        has_split_step = any(step.get('type') == 'split' for step in steps)

        if has_split_step:
            st.write("**Split Consistency After Transformation:**")

            # Find the split step and analyze part counts
            split_part_counts = []
            for val in analysis_data:
                temp_val = val
                for step in steps:
                    temp_val = preview_transformation_step(temp_val, step)
                    if step.get('type') == 'split' and isinstance(temp_val, list):
                        split_part_counts.append(len(temp_val))
                        break  # Count parts from first split only

            if split_part_counts:
                count_distribution = Counter(split_part_counts)
                total_records = len(split_part_counts)

                # Show distribution
                st.write("**Part count distribution:**")
                for part_count, frequency in sorted(count_distribution.items()):
                    percentage = (frequency / total_records) * 100
                    emoji = "✓" if percentage > 80 else "⚠️"
                    st.caption(f"  {emoji} {frequency:,} records ({percentage:.1f}%) split into **{part_count} parts**")

                # Warn about inconsistencies
                if len(count_distribution) > 1:
                    st.warning("⚠️ **Inconsistent split detected!** Records split into different numbers of parts.")

                    # Find outlier examples
                    most_common_count = count_distribution.most_common(1)[0][0]
                    outlier_examples = []

                    for val in analysis_data[:100]:  # Check first 100 for examples
                        temp_val = val
                        for step in steps:
                            temp_val = preview_transformation_step(temp_val, step)
                            if step.get('type') == 'split' and isinstance(temp_val, list):
                                if len(temp_val) != most_common_count:
                                    outlier_examples.append((val, temp_val))
                                break
                        if len(outlier_examples) >= 5:
                            break

                    if outlier_examples:
                        st.write(f"**Examples of outliers** (expected {most_common_count} parts):")
                        for orig, result in outlier_examples[:5]:
                            st.caption(f"  • `{orig}` → {result} ({len(result)} parts)")
                        st.info("💡 **Tip:** Review these outliers. You may need conditional logic or a different delimiter.")
                else:
                    st.success("✓ All records split consistently")

        # Show unique output values
        st.divider()
        st.subheader("Unique Output Values")

        # Check if output is still arrays/lists (not extracted yet)
        first_val = full_transformed.iloc[0] if len(full_transformed) > 0 else None
        if isinstance(first_val, list):
            st.info("ℹ️ **Output is still arrays** - Add an **Extract Part** step to get final values")
            st.caption("Use 'Extract Part' with index to select which element you want from the split")
        else:
            # Get unique values for non-list outputs
            try:
                unique_values = full_transformed.unique()
                unique_values_display = [v for v in unique_values if v is not None and v != '']

                if len(unique_values_display) > 0:
                    st.write(f"Found **{len(unique_values_display)}** unique values after transformation:")

                    # Show all unique values (or first 50 if too many)
                    display_limit = 50
                    if len(unique_values_display) <= display_limit:
                        values_display = ', '.join(f'`{v}`' for v in sorted(map(str, unique_values_display)))
                        st.markdown(values_display)
                    else:
                        values_display = ', '.join(f'`{v}`' for v in sorted(map(str, unique_values_display))[:display_limit])
                        st.markdown(values_display)
                        st.caption(f"... and {len(unique_values_display) - display_limit} more")

                    # Warn if there are too many unique values
                    uniqueness_ratio = len(unique_values_display) / len(analysis_data)
                    if uniqueness_ratio > 0.5 and len(analysis_data) > 10:
                        st.warning(f"⚠️ **High uniqueness ({uniqueness_ratio:.1%}):** Many different output values. Review to ensure transformation is working as expected.")
                    else:
                        st.success(f"✓ Uniqueness ratio: {uniqueness_ratio:.1%}")
                else:
                    st.caption("No valid output values found")
            except Exception as e:
                st.warning(f"Could not analyze unique values: {str(e)}")

    except Exception as e:
        st.error(f"Profile error: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

    # Close button
    if st.button("Close", use_container_width=True):
        st.session_state[f"profile_modal_open_{column_name}"] = False
        st.rerun()


# Page configuration
st.set_page_config(
    page_title="Data Template Manager",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Make the mapping section more distinct */
    .stSelectbox label {
        font-weight: 600 !important;
        font-size: 14px !important;
    }

    /* Better section separation */
    hr {
        margin: 1.5rem 0 !important;
        border-color: #e0e0e0 !important;
    }

    /* Compact column info table */
    .dataframe {
        font-size: 12px !important;
    }

    /* Red asterisk for required fields */
    .stSelectbox label {
        position: relative;
    }

    /* Make asterisks red */
    .stSelectbox label:after {
        content: '';
    }

    /* Target labels ending with asterisk and make them red */
    [data-testid="stSelectbox"] label span {
        color: inherit;
    }

    /* Style Custom option in dropdowns */
    [data-baseweb="select"] [role="option"]:first-child {
        color: #1f77b4 !important;
        font-style: italic !important;
        font-weight: 500 !important;
    }

    /* Highlight category prefixes in optional column dropdowns */
    [data-baseweb="select"] [role="option"] {
        font-family: 'SF Mono', Monaco, 'Courier New', monospace !important;
        font-size: 13px !important;
    }

    /* Style radio buttons to look more like tabs - only for the main navigation */
    .tab-navigation div[role="radiogroup"] {
        gap: 0.5rem;
        background-color: #f0f2f6;
        padding: 0.25rem;
        border-radius: 0.5rem;
    }

    .tab-navigation div[role="radiogroup"] label {
        background-color: transparent;
        padding: 0.5rem 1.5rem;
        border-radius: 0.375rem;
        cursor: pointer;
        transition: background-color 0.2s;
    }

    .tab-navigation div[role="radiogroup"] label:hover {
        background-color: rgba(255, 255, 255, 0.5);
    }

    .tab-navigation div[role="radiogroup"] label[data-baseweb="radio"] > div:first-child {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Required columns for viewership data
REQUIRED_COLUMNS = [
    "Partner",
    "Date",
    "Content Name",
    "Content ID",
    "Series",
    "Channel",
    "Territory",
    "Total Watch Time"
]

# Columns that can be specified at load time instead of in template
LOAD_TIME_COLUMNS = ["Channel", "Territory"]

# Available optional columns organized by category
AVAILABLE_OPTIONAL_COLUMNS = {
    "Metrics": [
        "AVG_DURATION_PER_SESSION",
        "AVG_DURATION_PER_VIEWER",
        "AVG_SESSION_COUNT",
        "CHANNEL_ADPOOL_IMPRESSIONS",
        "DURATION",
        "TOT_IMPRESSIONS",
        "TOT_COMPLETIONS",
        "TOT_SESSIONS",
        "UNIQUE_VIEWERS",
        "VIEWS"
    ],
    "Geo": [
        "CITY",
        "COUNTRY"
    ],
    "Device": [
        "DEVICE_ID",
        "DEVICE_NAME",
        "DEVICE_TYPE"
    ],
    "Content": [
        "EPISODE_NUMBER",
        "LANGUAGE",
        "CONTENT_PROVIDER",
        "REF_ID",
        "SEASON_NUMBER",
        "SERIES_CODE",
        "VIEWERSHIP_TYPE"
    ],
    "Date": [
        "END_TIME",
        "MONTH",
        "QUARTER",
        "START_TIME",
        "YEAR_MONTH_DAY",
        "YEAR"
    ],
    "Monetary": [
        "CHANNEL_ADPOOL_REVENUE",
        "REVENUE"
    ]
}

# Flatten the optional columns with section headers for dropdown
OPTIONAL_COLUMNS_LIST = []
SECTION_HEADERS = []  # Track section headers to exclude from selection
for category, columns in AVAILABLE_OPTIONAL_COLUMNS.items():
    # Add section header with gray line separator
    header = f"──── {category} ────"
    OPTIONAL_COLUMNS_LIST.append(header)
    SECTION_HEADERS.append(header)
    # Add columns without prefix
    for col in columns:
        OPTIONAL_COLUMNS_LIST.append(col)

# Default optional columns to add on first load
DEFAULT_OPTIONAL_COLUMNS = []

def init_session_state():
    """Initialize session state variables"""
    if 'uploaded_file' not in st.session_state:
        st.session_state.uploaded_file = None
    if 'df_preview' not in st.session_state:
        st.session_state.df_preview = None
    if 'column_mappings' not in st.session_state:
        st.session_state.column_mappings = {}
    if 'existing_config' not in st.session_state:
        st.session_state.existing_config = None
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    if 'config_id' not in st.session_state:
        st.session_state.config_id = None
    if 'optional_columns' not in st.session_state:
        st.session_state.optional_columns = []
    if 'optional_columns_loaded_from_config' not in st.session_state:
        st.session_state.optional_columns_loaded_from_config = False
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0

@st.cache_resource
def get_snowflake_connection():
    """
    Get or create a cached Snowflake connection.
    This connection is reused across all reruns and users to avoid re-authentication.

    The @st.cache_resource decorator ensures this function only runs once per app session,
    so we don't repeatedly authenticate to Snowflake.
    """
    try:
        conn = SnowflakeConnection()
        print(f"[Snowflake] New connection established for user: {st.secrets['snowflake']['user']}")
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        st.info("Please configure your Snowflake credentials in the .streamlit/secrets.toml file")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_platforms(_sf_conn):
    """Get platforms from Snowflake with caching"""
    return _sf_conn.get_platforms()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_channels(_sf_conn):
    """Get channels - hardcoded list"""
    return [
        "Nosey",
        "Confess by Nosey",
        "Judge Nosey",
        "Nosey Escandalos",
        "VOD"
    ]

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_territories(_sf_conn):
    """Get territories - hardcoded list"""
    return [
        "United States",
        "Canada",
        "India",
        "Mexico",
        "Australia",
        "New Zealand",
        "International",
        "Brazil"
    ]

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_partners(_sf_conn):
    """Get partners from dictionary.public.partners table"""
    return _sf_conn.get_partners()

def validate_connection(sf_conn):
    """
    Validate that the Snowflake connection is still alive.
    If connection is dead, clear the cache to force reconnection on next call.

    Args:
        sf_conn: The cached Snowflake connection

    Returns:
        True if connection is valid, False if it needs to be recreated
    """
    if sf_conn is None:
        return False

    if not sf_conn.is_connected():
        print("[Snowflake] Connection is dead, clearing cache to force reconnection")
        st.cache_resource.clear()
        return False

    return True

def main():
    """Main application"""
    init_session_state()

    # Display environment badge
    env_name = get_environment_name()
    env_colors = {
        'development': '🟢',
        'staging': '🟡',
        'production': '🔴'
    }
    env_badge = env_colors.get(env_name, '⚪')

    col1, col2 = st.columns([5, 1])
    with col1:
        st.title("📊 Data Template Manager")
        st.markdown("Manage data templates and column mappings for viewership data uploads")
    with col2:
        st.markdown(f"### {env_badge} {env_name.upper()}")
        st.caption("Environment")

    # Get cached Snowflake connection (only authenticates once per session)
    sf_conn = get_snowflake_connection()

    # Validate the connection is still alive
    if not validate_connection(sf_conn):
        # If connection is dead, try to get a new one
        sf_conn = get_snowflake_connection()
        if sf_conn is None:
            st.error("Unable to establish Snowflake connection. Please check your configuration.")
            return

    # Tabs for different functionalities with session state
    # Use radio buttons styled as tabs to preserve state across reruns
    tab_names = ["📤 Upload & Map", "🔍 Search & Edit", "📥 Load Data"]

    # Create a callback to update active tab
    def set_active_tab():
        st.session_state.active_tab = tab_names.index(st.session_state.selected_tab)

    # Wrap navigation in a container to apply specific styling
    st.markdown('<div class="tab-navigation">', unsafe_allow_html=True)
    selected = st.radio(
        "Navigation",
        tab_names,
        index=st.session_state.active_tab,
        horizontal=True,
        key="selected_tab",
        on_change=set_active_tab,
        label_visibility="collapsed"
    )
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    # Display the active tab content
    if st.session_state.active_tab == 0:
        upload_and_map_tab(sf_conn)
    elif st.session_state.active_tab == 1:
        search_and_edit_tab(sf_conn)
    elif st.session_state.active_tab == 2:
        load_data_tab(sf_conn)

def upload_and_map_tab(sf_conn):
    """Tab for uploading files and creating/editing mappings"""

    # Initialize variables
    df = None
    platform = ""
    partner = ""
    filename = ""

    # Only show upload section if no file is uploaded yet
    if 'df' not in st.session_state or st.session_state.get('df') is None:
        st.header("Upload File and Create Mapping")

        # Configuration details BEFORE file upload
        st.subheader("Configuration Details")

        # Fetch platforms from Snowflake (cached)
        platforms = get_cached_platforms(sf_conn)
        if platforms:
            platform = st.selectbox(
                "Platform *",
                options=[""] + platforms,
                help="Required. Select a platform from the list"
            )
        else:
            # Fallback to text input if query fails
            platform = st.text_input("Platform *", help="Required. e.g., YouTube, Netflix, Amazon Prime")

        # Partner dropdown (optional)
        partners = get_cached_partners(sf_conn)
        if partners:
            partner = st.selectbox(
                "Partner (optional)",
                options=[""] + partners,
                help="Optional. Select a partner from the list or leave blank for platform-wide template"
            )
        else:
            partner = st.text_input("Partner (optional)", help="Optional. Enter partner name or leave blank for platform-wide template")

        # Channel dropdown (optional)
        channels = get_cached_channels(sf_conn)
        if channels:
            channel = st.selectbox(
                "Channel (optional)",
                options=[""] + channels,
                help="Optional. Select a channel from the list"
            )
        else:
            channel = ""

        # Territory dropdown (optional)
        territories = get_cached_territories(sf_conn)
        if territories:
            territory = st.selectbox(
                "Territory (optional)",
                options=[""] + territories,
                help="Optional. Select a territory from the list"
            )
        else:
            territory = ""

        # Additional metadata fields
        domain = st.selectbox(
            "Domain",
            options=["", "Distribution Partners", "Owned and Operated"],
            help="Select domain type"
        )

        # Data Type selector - determines which columns are required
        # Map user-friendly labels to backend values
        data_type_labels = {
            "Hours/Mins by Episode": "Viewership",
            "Revenue by Episode": "Revenue"
        }

        data_type_display = st.selectbox(
            "Data Type *",
            options=["", "Hours/Mins by Episode", "Revenue by Episode"],
            help="Required. Select the type of data in this template. This determines which metrics columns are required."
        )

        # Convert display label to backend value
        data_type = data_type_labels.get(data_type_display, data_type_display) if data_type_display else ""

        # File uploader - disabled if no platform or data type
        if not platform or not data_type:
            st.info("⚠️ Please enter Platform name and Data Type before uploading a file")
        else:
            uploaded_file = st.file_uploader(
                "Upload a sample data file",
                type=['csv', 'xlsx', 'xls'],
                help="Upload a sample file to create column mappings"
            )

            # Read and store the dataframe in session state
            if uploaded_file is not None:
                try:
                    # Read file - try to detect if it's a wide format with multi-row header
                    if uploaded_file.name.endswith('.csv'):
                        # First, peek at the file to check structure
                        uploaded_file.seek(0)
                        first_lines = uploaded_file.read(500).decode('utf-8', errors='ignore')
                        uploaded_file.seek(0)

                        # Count how many date patterns appear in first line
                        import re
                        first_line = first_lines.split('\n')[0]
                        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
                        date_count = len(date_pattern.findall(first_line))

                        # If many dates in first line, likely wide format with multi-row header
                        # Read with first row as header
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)

                    # Detect and transform wide format (dates as columns) to long format (dates as rows)
                    df, was_transformed, filtered_count = detect_and_transform(df)

                    if was_transformed:
                        st.success("✅ Wide format detected and transformed to long format! Dates unpivoted from columns to rows.")
                        st.info(f"📊 Transformed {len(df)} records. You can now proceed with column mapping.")
                        if filtered_count > 0:
                            st.warning(f"⚠️ Filtered out {filtered_count} rows with no content identification (blank title/series). These records cannot be processed.")

                    st.session_state.df = df
                    st.session_state.filename = uploaded_file.name
                    st.session_state.platform = platform
                    st.session_state.partner = partner
                    st.session_state.channel = channel
                    st.session_state.territory = territory
                    st.session_state.domain = domain
                    st.session_state.data_type = data_type
                    st.session_state.was_wide_format = was_transformed
                    st.rerun()
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
    else:
        # File already uploaded, retrieve from session state
        df = st.session_state.df
        filename = st.session_state.get('filename', 'uploaded file')
        platform = st.session_state.get('platform', '')
        partner = st.session_state.get('partner', '')
        channel = st.session_state.get('channel', '')
        territory = st.session_state.get('territory', '')
        domain = st.session_state.get('domain', '')
        data_type = st.session_state.get('data_type', '')

    if df is not None:
        # Process the dataframe
        try:

            # Check for existing configuration - show at top if exists
            if platform:
                # Use "DEFAULT" as sentinel value for blank partner
                partner_value = partner if partner.strip() else "DEFAULT"
                existing_config = sf_conn.get_config_by_platform_partner(platform, partner_value)
                if existing_config:
                    partner_display = partner if partner.strip() else "(platform-wide)"
                    col_warn, col_load = st.columns([3, 1])
                    with col_warn:
                        st.warning(f"⚠️ Configuration exists for {platform} - {partner_display}. Load it or create new.", icon="⚠️")
                    with col_load:
                        if st.button("Load Existing", use_container_width=True):
                            st.session_state.existing_config = existing_config
                            st.session_state.edit_mode = True
                            st.session_state.config_id = existing_config['CONFIG_ID']

                            # Load sample data back into dataframe
                            if existing_config.get('SAMPLE_DATA'):
                                sample_df = pd.DataFrame(existing_config['SAMPLE_DATA'])
                                st.session_state.df = sample_df
                                st.session_state.filename = f"[Loaded from config] {platform} - {partner_display}"

                            # Load metadata into session state
                            st.session_state.channel = existing_config.get('CHANNEL', '')
                            st.session_state.territory = existing_config.get('TERRITORY', '')
                            st.session_state.domain = existing_config.get('DOMAIN', '')
                            st.session_state.data_type = existing_config.get('DATA_TYPE', '')

                            st.rerun()

            # New section header after upload with "Upload New File" button
            col_header, col_new = st.columns([4, 1])
            with col_header:
                st.header("📊 Map Columns")
                # Show user-friendly data type label
                data_type_reverse_map = {
                    "Viewership": "Hours/Mins by Episode",
                    "Revenue": "Revenue by Episode"
                }
                data_type_display_header = data_type_reverse_map.get(data_type, data_type) if data_type else '(not set)'
                st.markdown(f"**File:** {filename} • **Platform:** {platform} • **Partner:** {partner if partner else '(platform-wide)'} • **Type:** {data_type_display_header}")
            with col_new:
                if st.button("🔄 Upload New File", use_container_width=True):
                    st.session_state.df = None
                    st.session_state.filename = None
                    st.session_state.platform = None
                    st.session_state.partner = None
                    st.session_state.channel = None
                    st.session_state.territory = None
                    st.session_state.domain = None
                    st.session_state.data_type = None
                    st.session_state.edit_mode = False
                    st.session_state.config_id = None
                    st.session_state.existing_config = None
                    st.session_state.optional_columns = []
                    st.session_state.optional_columns_loaded_from_config = False
                    st.rerun()

            # Show editable metadata fields when file is uploaded
            st.subheader("Configuration Metadata")
            col_meta1, col_meta2 = st.columns(2)

            with col_meta1:
                # Channel dropdown
                channels = get_cached_channels(sf_conn)
                if channels:
                    channel_idx = 0
                    if channel and channel in channels:
                        channel_idx = channels.index(channel) + 1  # +1 for empty option
                    channel = st.selectbox(
                        "Channel (optional)",
                        options=[""] + channels,
                        index=channel_idx,
                        help="Optional. Select a channel from the list",
                        key="edit_channel"
                    )
                    # Update session state
                    st.session_state.channel = channel

            with col_meta2:
                # Territory dropdown
                territories = get_cached_territories(sf_conn)
                if territories:
                    territory_idx = 0
                    if territory and territory in territories:
                        territory_idx = territories.index(territory) + 1  # +1 for empty option
                    territory = st.selectbox(
                        "Territory (optional)",
                        options=[""] + territories,
                        index=territory_idx,
                        help="Optional. Select a territory from the list",
                        key="edit_territory"
                    )
                    # Update session state
                    st.session_state.territory = territory

            # Domain field (full width)
            domain_options = ["", "Distribution Partners", "Owned and Operated"]
            domain_idx = 0
            if domain and domain in domain_options:
                domain_idx = domain_options.index(domain)
            domain = st.selectbox(
                "Domain",
                options=domain_options,
                index=domain_idx,
                help="Select domain type",
                key="edit_domain"
            )
            # Update session state
            st.session_state.domain = domain

            # Data Type field (full width) - editable
            # Reverse map backend values to display labels
            data_type_reverse_map = {
                "Viewership": "Hours/Mins by Episode",
                "Revenue": "Revenue by Episode"
            }
            data_type_display_value = data_type_reverse_map.get(data_type, data_type) if data_type else ""

            data_type_display_options = ["", "Hours/Mins by Episode", "Revenue by Episode"]
            data_type_display_idx = 0
            if data_type_display_value and data_type_display_value in data_type_display_options:
                data_type_display_idx = data_type_display_options.index(data_type_display_value)

            data_type_display_edit = st.selectbox(
                "Data Type *",
                options=data_type_display_options,
                index=data_type_display_idx,
                help="Select the type of data in this template",
                key="edit_data_type"
            )

            # Map display label back to backend value
            data_type_labels = {
                "Hours/Mins by Episode": "Viewership",
                "Revenue by Episode": "Revenue"
            }
            data_type = data_type_labels.get(data_type_display_edit, data_type_display_edit) if data_type_display_edit else ""
            # Update session state
            st.session_state.data_type = data_type

            st.divider()

            # Two-column layout: Left = Data Preview, Right = Column Mapping
            left_col, right_col = st.columns([1, 1])

            # LEFT COLUMN: Data Preview and Column Information (Scrollable)
            with left_col:
                with st.container(height=800):
                    st.subheader("📋 Data Preview")
                    st.dataframe(df.head(50), use_container_width=True)

                    st.subheader("📊 Column Information")
                    col_info = []
                    for col in df.columns:
                        sample_values = df[col].dropna().head(3).tolist()
                        col_info.append({
                            "Column Name": col,
                            "Type": str(df[col].dtype),
                            "Non-Null": df[col].notna().sum(),
                            "Sample Values": ", ".join([str(v)[:30] for v in sample_values])
                        })

                    st.dataframe(pd.DataFrame(col_info), use_container_width=True)

            # RIGHT COLUMN: Column Mapping
            with right_col:
                st.subheader("🔗 Column Mapping")
                st.markdown("*Fields marked with <span style='color:red;'>*</span> are required*", unsafe_allow_html=True)

                mapper = ColumnMapper(df.columns.tolist(), REQUIRED_COLUMNS)

                # Load existing mappings if in edit mode
                if st.session_state.edit_mode and st.session_state.existing_config:
                    auto_mappings = st.session_state.existing_config['COLUMN_MAPPINGS']
                    # Only load optional columns from config ONCE (not on every rerun)
                    # This allows users to add/remove columns while editing
                    if 'optional_columns_loaded_from_config' not in st.session_state or not st.session_state.optional_columns_loaded_from_config:
                        st.session_state.optional_columns = [
                            col for col in auto_mappings.keys()
                            if col not in REQUIRED_COLUMNS and col not in SECTION_HEADERS
                        ]
                        st.session_state.optional_columns_loaded_from_config = True
                else:
                    auto_mappings = mapper.suggest_mappings()

                # Optional columns start empty - user can add them manually

                # Add Channel and Territory mappings to auto_mappings for intelligent suggestions
                all_columns_to_map = REQUIRED_COLUMNS + st.session_state.optional_columns
                mapper_all = ColumnMapper(df.columns.tolist(), all_columns_to_map)
                auto_mappings_all = mapper_all.suggest_mappings()

                final_mappings = {}

                # REQUIRED COLUMNS (with red asterisks)
                for required_col in REQUIRED_COLUMNS:
                    # Use saved mappings if in edit mode, otherwise use auto-suggestions
                    if st.session_state.edit_mode and st.session_state.existing_config:
                        saved_mapping = auto_mappings.get(required_col, "")
                        # Handle both old format (string) and new format (dict with source_column)
                        if isinstance(saved_mapping, dict) and 'source_column' in saved_mapping:
                            suggested = saved_mapping['source_column']
                            # Restore transformation if it exists
                            if 'transformation' in saved_mapping:
                                transform_key = f"transform_config_{required_col}"
                                applied_key = f"transform_applied_{required_col}"
                                st.session_state[transform_key] = saved_mapping['transformation']
                                st.session_state[applied_key] = True
                        else:
                            suggested = saved_mapping
                    else:
                        suggested = auto_mappings_all.get(required_col, "")

                    # Create dropdown with Custom first, then columns, then ❌ Not Mapped
                    options = ["✏️ Custom (enter manually)"] + df.columns.tolist() + ["❌ Not Mapped"]
                    default_idx = 0
                    if suggested:
                        if suggested in df.columns.tolist():
                            # Suggested column exists in data
                            default_idx = options.index(suggested)
                        elif suggested != "":
                            # Suggested value is a custom/hardcoded value (not a column)
                            default_idx = 0  # Select "Custom"

                    # Show label with asterisk - red for truly required, blue for load-time optional
                    if required_col in LOAD_TIME_COLUMNS:
                        st.markdown(f"**{required_col}** <span style='color: #0066cc; font-weight: bold;'>*</span>", unsafe_allow_html=True)
                        st.caption("💡 Can be left unmapped and specified at load time")
                    else:
                        st.markdown(f"**{required_col}** <span style='color: red; font-weight: bold;'>*</span>", unsafe_allow_html=True)
                    selected = st.selectbox(
                        required_col,
                        options,
                        index=default_idx,
                        key=f"mapping_{required_col}",
                        help=f"Required: Map a source column to {required_col}",
                        label_visibility="collapsed"
                    )

                    # Show transformation options if a column is selected
                    transformation_config = None
                    if selected != "❌ Not Mapped" and selected != "✏️ Custom (enter manually)":
                        # Check if transformation was applied
                        transform_key = f"transform_config_{required_col}"
                        applied_key = f"transform_applied_{required_col}"

                        # Show button to open transformation builder
                        col_btn, col_status = st.columns([1, 2])
                        with col_btn:
                            if st.button("🔧 Transform", key=f"open_transform_{required_col}"):
                                st.session_state[f"modal_open_{required_col}"] = True
                                # Keep small sample for fast preview (10 rows)
                                st.session_state[f"modal_sample_data_{required_col}"] = df[selected].head(10).tolist()
                                # Store full column for profiling
                                st.session_state[f"full_data_{required_col}"] = df[selected].tolist()

                        # Keep modal open if it should be open
                        if st.session_state.get(f"modal_open_{required_col}", False):
                            sample_values = st.session_state.get(f"modal_sample_data_{required_col}", df[selected].head(10).tolist())
                            full_data = st.session_state.get(f"full_data_{required_col}", df[selected].tolist())
                            transformation_builder_modal(required_col, sample_values, full_data)

                        # Open profile modal if requested
                        if st.session_state.get(f"profile_modal_open_{required_col}", False):
                            full_data = st.session_state.get(f"full_data_for_profile_{required_col}", df[selected].tolist())
                            transformation_config = st.session_state.get(f"profile_transformation_{required_col}")
                            # Get steps from session state
                            steps = st.session_state.get(f"profile_steps_{required_col}", [])
                            profile_data_modal(required_col, transformation_config, full_data, steps)

                        with col_status:
                            # Show transformation status
                            if applied_key in st.session_state and st.session_state[applied_key]:
                                transformation_config = st.session_state.get(transform_key)
                                if transformation_config:
                                    st.caption("✅ Transformation applied")
                                else:
                                    st.caption("🔧 Click Transform to add")
                            else:
                                st.caption("🔧 Click Transform to add transformations")

                        # Retrieve the transformation config if it was applied
                        if applied_key in st.session_state and st.session_state[applied_key]:
                            transformation_config = st.session_state.get(transform_key)

                    # Special handling for Total Watch Time - add unit selector
                    if required_col == "Total Watch Time" and selected != "❌ Not Mapped":
                        if not transformation_config:  # Only show if no transformation applied
                            st.caption("Select unit:")

                            # Get saved unit if in edit mode
                            default_unit_idx = 0  # Default to "Hours"
                            if st.session_state.edit_mode and st.session_state.existing_config:
                                saved_mapping = auto_mappings.get(required_col, "")
                                if isinstance(saved_mapping, dict) and 'unit' in saved_mapping:
                                    saved_unit = saved_mapping['unit'].lower()
                                    if saved_unit == 'minutes':
                                        default_unit_idx = 1

                            unit = st.radio(
                                "Unit",
                                options=["Hours", "Minutes"],
                                index=default_unit_idx,
                                horizontal=True,
                                key=f"unit_{required_col}",
                                label_visibility="collapsed"
                            )

                    # If "Custom" is selected, show text input
                    if selected == "✏️ Custom (enter manually)":
                        # Pre-fill with saved value if in edit mode and value is not a column
                        default_custom_value = ""
                        if suggested and suggested not in df.columns.tolist():
                            default_custom_value = suggested

                        custom_value = st.text_input(
                            f"Enter custom value for {required_col}",
                            value=default_custom_value,
                            key=f"custom_{required_col}",
                            placeholder="Type column name or expression",
                            label_visibility="collapsed"
                        )
                        if custom_value:
                            # For Total Watch Time, store unit in a separate key
                            if required_col == "Total Watch Time":
                                final_mappings[required_col] = custom_value
                                final_mappings['_total_watch_time_unit'] = unit.lower()
                            else:
                                final_mappings[required_col] = custom_value
                    elif selected != "❌ Not Mapped":
                        # Store mapping with optional transformation
                        mapping_value = {
                            'source_column': selected
                        }

                        # Add transformation if configured
                        if transformation_config:
                            mapping_value['transformation'] = transformation_config

                        # For Total Watch Time, store unit
                        if required_col == "Total Watch Time" and not transformation_config:
                            mapping_value['unit'] = unit.lower()

                        final_mappings[required_col] = mapping_value

                # OPTIONAL COLUMNS SECTION
                st.markdown("---")
                st.markdown("**Optional Columns**")
                st.caption("💡 Start typing to search for a field")

                # Clean up any section headers that might have snuck into optional_columns
                st.session_state.optional_columns = [
                    col for col in st.session_state.optional_columns
                    if col not in SECTION_HEADERS
                ]

                # Display existing optional columns
                for idx, opt_col in enumerate(st.session_state.optional_columns):
                    # Get current column index in the list, or use current value
                    current_idx = 0
                    if opt_col in OPTIONAL_COLUMNS_LIST:
                        current_idx = OPTIONAL_COLUMNS_LIST.index(opt_col)

                    col_label, col_dropdown, col_remove = st.columns([2, 3, 1])

                    with col_label:
                        updated_label = st.selectbox(
                            "Field Name",
                            options=OPTIONAL_COLUMNS_LIST,
                            index=current_idx,
                            key=f"opt_label_{idx}",
                            label_visibility="collapsed"
                        )
                        # Update the label in session state if changed (but never allow section headers)
                        if updated_label != opt_col and updated_label not in SECTION_HEADERS:
                            st.session_state.optional_columns[idx] = updated_label
                        elif updated_label in SECTION_HEADERS:
                            # If user somehow selected a section header, keep the old value
                            updated_label = opt_col

                    # Use updated label if changed
                    current_label = updated_label

                    with col_dropdown:
                        # Extract base column name (remove category prefix) for mapping
                        base_col_name = current_label.split(" - ")[-1] if " - " in current_label else current_label

                        # Try to get suggestion - use saved mappings if in edit mode
                        if st.session_state.edit_mode and st.session_state.existing_config:
                            saved_mapping = auto_mappings.get(base_col_name, "")
                            # Handle both old format (string) and new format (dict with source_column)
                            if isinstance(saved_mapping, dict) and 'source_column' in saved_mapping:
                                opt_suggested = saved_mapping['source_column']
                                # Restore transformation if it exists
                                if 'transformation' in saved_mapping:
                                    opt_transform_key = f"opt_transform_config_{idx}"
                                    opt_applied_key = f"opt_transform_applied_{idx}"
                                    st.session_state[opt_transform_key] = saved_mapping['transformation']
                                    st.session_state[opt_applied_key] = True
                            else:
                                opt_suggested = saved_mapping
                        else:
                            opt_suggested = auto_mappings_all.get(base_col_name, "")

                        opt_options = ["✏️ Custom (enter manually)"] + df.columns.tolist() + ["❌ Not Mapped"]
                        opt_default_idx = 0
                        if opt_suggested:
                            if opt_suggested in df.columns.tolist():
                                # Suggested column exists in data
                                opt_default_idx = opt_options.index(opt_suggested)
                            elif opt_suggested != "":
                                # Suggested value is a custom/hardcoded value (not a column)
                                opt_default_idx = 0  # Select "Custom"

                        opt_selected = st.selectbox(
                            "Source",
                            opt_options,
                            index=opt_default_idx,
                            key=f"opt_mapping_{idx}",
                            label_visibility="collapsed"
                        )

                        # Show transformation options for optional columns
                        opt_transformation_config = None
                        if opt_selected != "❌ Not Mapped" and opt_selected != "✏️ Custom (enter manually)":
                            # Use a unique key for this optional column
                            opt_column_key = f"opt_{idx}_{current_label}"
                            opt_transform_key = f"transform_config_{opt_column_key}"
                            opt_applied_key = f"transform_applied_{opt_column_key}"

                            # Show button to open transformation builder
                            col_btn2, col_status2 = st.columns([1, 2])
                            with col_btn2:
                                if st.button("🔧", key=f"open_transform_opt_{idx}"):
                                    st.session_state[f"modal_open_{opt_column_key}"] = True
                                    st.session_state[f"modal_sample_data_{opt_column_key}"] = df[opt_selected].head(10).tolist()
                                    st.session_state[f"full_data_{opt_column_key}"] = df[opt_selected].tolist()

                            # Keep modal open if it should be open
                            if st.session_state.get(f"modal_open_{opt_column_key}", False):
                                opt_sample_values = st.session_state.get(f"modal_sample_data_{opt_column_key}", df[opt_selected].head(10).tolist())
                                opt_full_data = st.session_state.get(f"full_data_{opt_column_key}", df[opt_selected].tolist())
                                transformation_builder_modal(opt_column_key, opt_sample_values, opt_full_data)

                            # Open profile modal if requested
                            if st.session_state.get(f"profile_modal_open_{opt_column_key}", False):
                                opt_full_data = st.session_state.get(f"full_data_for_profile_{opt_column_key}", df[opt_selected].tolist())
                                opt_transformation_config = st.session_state.get(f"profile_transformation_{opt_column_key}")
                                # Get steps from session state
                                opt_steps = st.session_state.get(f"profile_steps_{opt_column_key}", [])
                                profile_data_modal(opt_column_key, opt_transformation_config, opt_full_data, opt_steps)

                            with col_status2:
                                # Show transformation status
                                if opt_applied_key in st.session_state and st.session_state[opt_applied_key]:
                                    opt_transformation_config = st.session_state.get(opt_transform_key)
                                    if opt_transformation_config:
                                        st.caption("✅ Transformed")
                                else:
                                    st.caption("")  # Empty for alignment

                            # Retrieve the transformation config if it was applied
                            if opt_applied_key in st.session_state and st.session_state[opt_applied_key]:
                                opt_transformation_config = st.session_state.get(opt_transform_key)

                        # If "Custom" is selected, show text input
                        if opt_selected == "✏️ Custom (enter manually)":
                            # Pre-fill with saved value if in edit mode and value is not a column
                            opt_default_custom_value = ""
                            if opt_suggested and opt_suggested not in df.columns.tolist():
                                opt_default_custom_value = opt_suggested

                            opt_custom_value = st.text_input(
                                f"Enter custom value",
                                value=opt_default_custom_value,
                                key=f"opt_custom_{idx}",
                                placeholder="Type column name",
                                label_visibility="collapsed"
                            )
                            if opt_custom_value:
                                # Store with full label (including category)
                                final_mappings[current_label] = opt_custom_value
                        elif opt_selected != "❌ Not Mapped":
                            # Store with full label and optional transformation
                            mapping_value = {
                                'source_column': opt_selected
                            }
                            if opt_transformation_config:
                                mapping_value['transformation'] = opt_transformation_config
                            final_mappings[current_label] = mapping_value

                    with col_remove:
                        if st.button("🗑️", key=f"remove_{idx}", help="Remove this column"):
                            st.session_state.optional_columns.pop(idx)
                            st.rerun()

                # Add new optional column button
                col_add, col_spacer = st.columns([1, 2])
                with col_add:
                    # Count how many columns are available to add
                    available_cols = [
                        col for col in OPTIONAL_COLUMNS_LIST
                        if col not in st.session_state.optional_columns and col not in SECTION_HEADERS
                    ]

                    button_label = f"➕ Add Optional Column ({len(available_cols)} available)"
                    if st.button(button_label, use_container_width=True, disabled=(len(available_cols) == 0)):
                        # Add the first available column that hasn't been added yet (skip section headers)
                        if available_cols:
                            st.session_state.optional_columns.append(available_cols[0])
                            st.rerun()

            # Save configuration
            st.subheader("Save Configuration")

            if not platform:
                st.warning("Please enter Platform to save the configuration.")
            else:
                col1, col2, col3 = st.columns([1, 1, 2])

                with col1:
                    if st.button("💾 Save Configuration", type="primary", use_container_width=True):
                        try:
                            # Use "DEFAULT" for blank partner
                            partner_value = partner.strip() if partner.strip() else "DEFAULT"

                            # Save first 100 rows as sample data
                            sample_df = df.head(100)
                            sample_data = sample_df.to_dict(orient='records')

                            config_data = {
                                "platform": platform,
                                "partner": partner_value,
                                "channel": channel if channel else None,  # NULL for platform-wide configs
                                "territory": territory if territory else None,  # NULL for platform-wide configs
                                "domain": domain if domain else None,
                                "data_type": data_type if data_type else None,
                                "column_mappings": final_mappings,
                                "filename_pattern": None,
                                "source_columns": df.columns.tolist(),
                                "sample_data": sample_data,
                                "created_date": datetime.now().isoformat()
                            }

                            if st.session_state.edit_mode and st.session_state.config_id:
                                sf_conn.update_config(st.session_state.config_id, config_data)
                                st.success("Configuration updated successfully!")

                                # Reset session state
                                st.session_state.edit_mode = False
                                st.session_state.config_id = None
                                st.session_state.existing_config = None
                                st.session_state.optional_columns = []
                                st.session_state.optional_columns_loaded_from_config = False
                                st.session_state.df = None
                                st.session_state.filename = None
                                st.session_state.platform = None
                                st.session_state.partner = None
                                st.session_state.channel = None
                                st.session_state.territory = None
                                st.session_state.domain = None
                                st.session_state.data_type = None
                            else:
                                # Check if exact config already exists (matching UNIQUE constraint)
                                existing_config = sf_conn.check_duplicate_config(
                                    platform=platform,
                                    partner=partner_value,
                                    channel=channel,
                                    territory=territory
                                )

                                if existing_config:
                                    # Config exists - show warning
                                    st.warning(f"⚠️ Configuration already exists for Platform: {platform}, Partner: {partner_value}, Channel: {channel or '(none)'}, Territory: {territory or '(none)'}")
                                    st.info("Would you like to **update** the existing configuration instead?")

                                    col_yes, col_no = st.columns(2)
                                    with col_yes:
                                        if st.button("✅ Yes, Update It", use_container_width=True):
                                            sf_conn.update_config(existing_config['config_id'], config_data)
                                            st.success("Configuration updated successfully!")

                                            # Reset session state
                                            st.session_state.edit_mode = False
                                            st.session_state.config_id = None
                                            st.session_state.existing_config = None
                                            st.session_state.optional_columns = []
                                            st.session_state.optional_columns_loaded_from_config = False
                                            st.session_state.df = None
                                            st.session_state.filename = None
                                            st.session_state.platform = None
                                            st.session_state.partner = None
                                            st.session_state.channel = None
                                            st.session_state.territory = None
                                            st.session_state.domain = None
                                            st.session_state.data_type = None
                                            st.rerun()
                                    with col_no:
                                        if st.button("❌ Cancel", use_container_width=True):
                                            st.info("Save cancelled. You can edit the existing config in the 'Search & Edit' tab.")
                                else:
                                    # No existing config - safe to insert
                                    sf_conn.insert_config(config_data)
                                    partner_display = partner if partner.strip() else "(platform-wide)"
                                    st.success(f"Configuration saved successfully for Platform: {platform}, Partner: {partner_display}")

                                    # Reset session state
                                    st.session_state.edit_mode = False
                                    st.session_state.config_id = None
                                    st.session_state.existing_config = None
                                    st.session_state.optional_columns = []
                                    st.session_state.optional_columns_loaded_from_config = False
                                    st.session_state.df = None
                                    st.session_state.filename = None
                                    st.session_state.platform = None
                                    st.session_state.partner = None
                                    st.session_state.channel = None
                                    st.session_state.territory = None
                                    st.session_state.domain = None
                                    st.session_state.data_type = None

                        except Exception as e:
                            st.error(f"Error saving configuration: {str(e)}")

                with col2:
                    if st.button("🔄 Reset", use_container_width=True):
                        st.session_state.edit_mode = False
                        st.session_state.config_id = None
                        st.session_state.existing_config = None
                        st.session_state.optional_columns = []
                        st.session_state.optional_columns_loaded_from_config = False
                        st.session_state.df = None
                        st.session_state.filename = None
                        st.session_state.platform = None
                        st.session_state.partner = None
                        st.session_state.channel = None
                        st.session_state.territory = None
                        st.session_state.domain = None
                        st.session_state.data_type = None
                        st.rerun()

        except Exception as e:
            st.error(f"Error reading file: {str(e)}")

def search_and_edit_tab(sf_conn):
    """Tab for searching and editing existing configurations"""
    st.header("Search & Edit Configurations")

    # Search options
    search_method = st.radio(
        "Search by:",
        ["Platform & Partner", "View All"],
        horizontal=True
    )

    if search_method == "Platform & Partner":
        col1, col2, col3 = st.columns([2, 2, 1])

        with col1:
            search_platform = st.text_input("Platform")
        with col2:
            partners = get_cached_partners(sf_conn)
            if partners:
                search_partner = st.selectbox(
                    "Partner",
                    options=[""] + partners
                )
            else:
                search_partner = st.text_input("Partner")
        with col3:
            st.write("")  # Spacer
            st.write("")  # Spacer
            search_button = st.button("🔍 Search", type="primary")

        if search_button and (search_platform or search_partner):
            configs = sf_conn.search_configs(search_platform, search_partner)
            display_configs(configs, sf_conn)
        elif search_button:
            st.warning("Please enter at least one search criterion.")

    else:  # View All
        configs = sf_conn.get_all_configs()
        display_configs(configs, sf_conn)

def display_configs(configs, sf_conn):
    """Display configurations in a table with edit/delete options"""
    if not configs:
        st.info("No configurations found.")
        return

    st.subheader(f"Found {len(configs)} configuration(s)")

    for idx, config in enumerate(configs):
        # Display partner as "(platform-wide)" if it's "DEFAULT"
        partner_display = config['PARTNER'] if config['PARTNER'] != 'DEFAULT' else '(platform-wide)'

        with st.expander(f"📄 {config['PLATFORM']} - {partner_display} (ID: {config['CONFIG_ID']})"):
            col1, col2 = st.columns([3, 1])

            with col1:
                st.markdown(f"**Platform:** {config['PLATFORM']}")
                st.markdown(f"**Partner:** {partner_display}")
                if config.get('CHANNEL'):
                    st.markdown(f"**Channel:** {config['CHANNEL']}")
                if config.get('TERRITORY'):
                    st.markdown(f"**Territory:** {config['TERRITORY']}")
                if config.get('DOMAIN'):
                    st.markdown(f"**Domain:** {config['DOMAIN']}")

                # Show data type with user-friendly label
                if config.get('DATA_TYPE'):
                    data_type_reverse_map = {
                        "Viewership": "Hours/Mins by Episode",
                        "Revenue": "Revenue by Episode"
                    }
                    data_type_display = data_type_reverse_map.get(config.get('DATA_TYPE'), config.get('DATA_TYPE'))
                    st.markdown(f"**Data Type:** {data_type_display}")

                st.markdown(f"**Created:** {config['CREATED_DATE']}")

                st.markdown("**Column Mappings:**")
                # Normalize values for display (handle both string and dict formats)
                mappings_list = []
                for k, v in config['COLUMN_MAPPINGS'].items():
                    if isinstance(v, dict):
                        # Extract source column and add transformation indicator
                        source = v.get('source_column', str(v))
                        if 'transformation' in v:
                            source = f"{source} (transformed)"
                        mappings_list.append({"Required Field": k, "Mapped Column": source})
                    else:
                        # Simple string value
                        mappings_list.append({"Required Field": k, "Mapped Column": str(v)})

                mappings_df = pd.DataFrame(mappings_list)
                st.dataframe(mappings_df, use_container_width=True, hide_index=True)

                if config.get('SOURCE_COLUMNS'):
                    with st.expander("View Source Columns"):
                        st.write(config['SOURCE_COLUMNS'])

            with col2:
                st.write("")  # Spacer
                if st.button("✏️ Edit", key=f"edit_{config['CONFIG_ID']}", use_container_width=True):
                    st.session_state.existing_config = config
                    st.session_state.edit_mode = True
                    st.session_state.config_id = config['CONFIG_ID']

                    # Load sample data back into dataframe for editing
                    if config.get('SAMPLE_DATA'):
                        sample_df = pd.DataFrame(config['SAMPLE_DATA'])
                        st.session_state.df = sample_df
                        partner_display = config['PARTNER'] if config['PARTNER'] != 'DEFAULT' else "(platform-wide)"
                        st.session_state.filename = f"[Loaded from config] {config['PLATFORM']} - {partner_display}"
                        st.session_state.platform = config['PLATFORM']
                        st.session_state.partner = config['PARTNER'] if config['PARTNER'] != 'DEFAULT' else ""
                        st.session_state.channel = config.get('CHANNEL', '')
                        st.session_state.territory = config.get('TERRITORY', '')
                        st.session_state.domain = config.get('DOMAIN', '')
                        st.session_state.data_type = config.get('DATA_TYPE', '')

                    # Switch to Upload & Map tab
                    st.session_state.active_tab = 0
                    st.rerun()

                if st.button("🗑️ Delete", key=f"delete_{config['CONFIG_ID']}", use_container_width=True):
                    try:
                        sf_conn.delete_config(config['CONFIG_ID'])
                        st.success("Configuration deleted successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting configuration: {str(e)}")

def load_data_tab(sf_conn):
    """Tab for loading data into platform_viewership table using existing templates"""

    st.header("Load Data to Platform Viewership")
    st.write("Select a configuration template and upload data to load into the platform_viewership table.")

    # Create centered, narrower container
    col_left, col_center, col_right = st.columns([1, 2, 1])

    with col_center:
        # Section 1: Find Configuration
        st.subheader("1. Find Configuration Template")

        # Platform at the top
        platforms = get_cached_platforms(sf_conn)
        if platforms:
            platform = st.selectbox(
                "Platform *",
                options=[""] + platforms,
                help="Required. Select the platform for this data"
            )
        else:
            platform = st.text_input("Platform *", help="Required. Enter the platform name")

        # Partner (optional)
        partners = get_cached_partners(sf_conn)
        if partners:
            partner = st.selectbox(
                "Partner (optional)",
                options=[""] + partners,
                help="Optional. Select a partner from the list or leave blank for platform-wide template"
            )
        else:
            partner = st.text_input("Partner (optional)", help="Optional. Enter partner name or leave blank for platform-wide template")

        # Channel dropdown (optional)
        channels = get_cached_channels(sf_conn)
        if channels:
            channel = st.selectbox(
                "Channel (optional)",
                options=[""] + channels,
                help="Optional. Filter by channel"
            )
        else:
            channel = ""

        # Territory dropdown (optional)
        territories = get_cached_territories(sf_conn)
        if territories:
            territory = st.selectbox(
                "Territory (optional)",
                options=[""] + territories,
                help="Optional. Filter by territory"
            )
        else:
            territory = ""

        # Year (required)
        year = st.selectbox(
            "Year *",
            options=[2025, 2026],
            help="Required. Select the year for this data"
        )

        # Quarter (required)
        quarter = st.selectbox(
            "Quarter *",
            options=["Q1", "Q2", "Q3", "Q4"],
            index=2,  # Default to Q3
            help="Required. Select the quarter for this data"
        )

        # Month (optional)
        month = st.selectbox(
            "Month (optional)",
            options=["", "January", "February", "March", "April", "May", "June",
                     "July", "August", "September", "October", "November", "December"],
            help="Optional. Select the month for this data"
        )

        # Search for configuration
        config = None
        if platform:
            try:
                # Use DEFAULT if partner is blank
                partner_value = partner.strip() if partner.strip() else "DEFAULT"
                config = sf_conn.get_config_by_platform_partner(platform, partner_value)

                if config:
                    st.success(f"✓ Found configuration for Platform: {platform}, Partner: {partner_value}")

                    # Show config details in expander
                    with st.expander("View Template Details", expanded=False):
                        st.write("**Platform:**", config.get('PLATFORM'))
                        st.write("**Partner:**", config.get('PARTNER'))
                        if config.get('CHANNEL'):
                            st.write("**Channel:**", config.get('CHANNEL'))
                        if config.get('TERRITORY'):
                            st.write("**Territory:**", config.get('TERRITORY'))

                        # Show data type with user-friendly label
                        if config.get('DATA_TYPE'):
                            data_type_reverse_map = {
                                "Viewership": "Hours/Mins by Episode",
                                "Revenue": "Revenue by Episode"
                            }
                            data_type_display = data_type_reverse_map.get(config.get('DATA_TYPE'), config.get('DATA_TYPE'))
                            st.write("**Data Type:**", data_type_display)

                        st.write("**Created:**", config.get('CREATED_DATE'))
                        if config.get('UPDATED_DATE'):
                            st.write("**Updated:**", config.get('UPDATED_DATE'))

                        st.write("**Column Mappings:**")
                        st.json(config.get('COLUMN_MAPPINGS', {}))
                else:
                    st.warning(f"No configuration found for Platform: {platform}, Partner: {partner_value}")
                    st.info("Please create a template first in the 'Upload & Map' tab.")
            except Exception as e:
                st.error(f"Error searching for configuration: {str(e)}")

        # Section 2: Upload and Load Data
        if config:
            # Get metadata from config
            domain = config.get('DOMAIN', '')
            data_type = config.get('DATA_TYPE', '')

            # User email for notifications - remember in session state
            if 'user_email' not in st.session_state:
                st.session_state.user_email = 'tayloryoung@mvmediasales.com'  # Default email

            user_email = st.text_input(
                "Your Email (for notifications)",
                value=st.session_state.user_email,
                help="Enter your email address for notifications",
                key="notification_email"
            )
            # Update session state when changed
            st.session_state.user_email = user_email

            # Debug mode toggle
            debug_mode = st.checkbox(
                "🐛 DEBUG MODE - Load only first 100 rows",
                value=False,
                help="Enable this to test with only the first 100 rows of each file. Much faster for testing!"
            )
            if debug_mode:
                st.warning("⚠️ DEBUG MODE ACTIVE: Only the first 100 rows of each file will be loaded. This is for testing only!")

            st.subheader("2. Upload Data Files")

            uploaded_files = st.file_uploader(
                "Upload data file(s) to load",
                type=['csv', 'xlsx', 'xls'],
                help="Upload one or more files matching the template format",
                key="load_data_uploader",
                accept_multiple_files=True
            )

            if uploaded_files:
                st.write(f"**Files Selected:** {len(uploaded_files)}")

                # Show summary of all files
                total_rows = 0
                file_info = []

                for uploaded_file in uploaded_files:
                    try:
                        # Read the file
                        if uploaded_file.name.endswith('.csv'):
                            df = pd.read_csv(uploaded_file)
                        else:
                            df = pd.read_excel(uploaded_file)

                        # Detect and transform wide format (dates as columns) to long format (dates as rows)
                        df, was_transformed, filtered_count = detect_and_transform(df)

                        if was_transformed:
                            st.info(f"📊 Wide format detected in {uploaded_file.name} - transformed to long format ({len(df)} records)")
                            if filtered_count > 0:
                                st.warning(f"⚠️ Filtered out {filtered_count} rows from {uploaded_file.name} with no content identification (blank title/series)")

                        # Apply debug mode limit if enabled
                        if debug_mode:
                            original_count = len(df)
                            df = df.head(100)
                            st.info(f"🐛 DEBUG MODE: Limited {uploaded_file.name} from {original_count:,} to {len(df)} rows")

                        file_info.append({
                            'name': uploaded_file.name,
                            'rows': len(df),
                            'columns': len(df.columns),
                            'df': df
                        })
                        total_rows += len(df)
                    except Exception as e:
                        st.error(f"Error reading {uploaded_file.name}: {str(e)}")

                if file_info:
                    # Show file summary
                    st.write(f"**Total Rows Across All Files:** {total_rows:,}")

                    with st.expander("View File Details", expanded=False):
                        for info in file_info:
                            st.write(f"**{info['name']}** - {info['rows']:,} rows, {info['columns']} columns")

                    # Preview first file - show transformed data
                    with st.expander(f"Preview Transformed Data from {file_info[0]['name']}", expanded=True):
                        try:
                            # Apply transformations to preview
                            column_mappings = config.get('COLUMN_MAPPINGS', {})
                            preview_df = apply_column_mappings(
                                file_info[0]['df'].head(50),
                                column_mappings,
                                platform,
                                channel,
                                territory,
                                domain,
                                file_info[0]['name'],
                                year,
                                quarter,
                                month
                            )

                            st.caption("📊 This shows what will be loaded to Snowflake (after transformations)")
                            st.dataframe(preview_df.head(10), use_container_width=True)

                            # Show summary
                            st.caption(f"**Preview:** First 10 of {len(preview_df):,} rows that will be loaded")
                        except Exception as e:
                            st.warning(f"Could not generate transformed preview: {str(e)}")
                            st.caption("Showing raw data instead:")
                            st.dataframe(file_info[0]['df'].head(10), use_container_width=True)

                    # Apply mappings and load
                    st.subheader("3. Load Data")

                    if st.button("🚀 Load All Files to Platform Viewership", type="primary", use_container_width=True):
                        try:
                            # Get column mappings
                            column_mappings = config.get('COLUMN_MAPPINGS', {})

                            # Process each file
                            total_loaded = 0
                            total_hov = 0.0
                            all_transformed_dfs = []
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            batch_status = st.empty()

                            for idx, info in enumerate(file_info):
                                status_text.text(f"Processing {info['name']}... ({idx + 1}/{len(file_info)})")
                                batch_status.text("")

                                try:
                                    # Transform data according to mappings
                                    transformed_df = apply_column_mappings(info['df'], column_mappings, platform, channel, territory, domain, info['name'], year, quarter, month)
                                    all_transformed_dfs.append(transformed_df)

                                    # Calculate total hours of viewership
                                    if 'TOT_HOV' in transformed_df.columns:
                                        total_hov += transformed_df['TOT_HOV'].sum()
                                    elif 'TOT_MOV' in transformed_df.columns:
                                        # Convert minutes to hours
                                        total_hov += transformed_df['TOT_MOV'].sum() / 60.0

                                    # Define progress callback for batch updates
                                    def batch_progress(batch_num, total_batches, rows_in_batch):
                                        batch_status.text(f"  └─ Batch {batch_num}/{total_batches}: Inserted {rows_in_batch:,} rows")

                                    # Load to Snowflake with batch progress
                                    rows_loaded = sf_conn.load_to_platform_viewership(transformed_df, progress_callback=batch_progress)
                                    total_loaded += rows_loaded

                                    batch_status.text("")  # Clear batch status
                                    st.success(f"✓ {info['name']}: {rows_loaded:,} rows loaded")

                                except Exception as e:
                                    st.error(f"✗ {info['name']}: {str(e)}")

                                # Update progress
                                progress_bar.progress((idx + 1) / len(file_info))

                            status_text.text("Complete!")
                            st.success(f"🎉 Successfully loaded {total_loaded:,} total rows from {len(file_info)} file(s) to platform_viewership table!")

                            # Invoke Lambda function after successful upload (if enabled)
                            config = get_config()
                            if config.ENABLE_LAMBDA:
                                try:
                                    st.info("🔄 Triggering post-processing workflow (asset matching + table migration)...")

                                    # Load AWS config based on environment
                                    aws_config = load_aws_config()

                                    # Configure AWS Lambda client
                                    lambda_client = boto3.client(
                                        'lambda',
                                        aws_access_key_id=aws_config['access_key_id'],
                                        aws_secret_access_key=aws_config['secret_access_key'],
                                        region_name=aws_config['region']
                                    )

                                    # Invoke Lambda once per file (not once for all files with concatenated filenames)
                                    lambda_success_count = 0
                                    for file_idx, transformed_df in enumerate(all_transformed_dfs):
                                        filename = file_info[file_idx]['name']
                                        record_count = len(transformed_df)

                                        # Calculate TOT_HOV for this specific file
                                        file_hov = 0.0
                                        if 'TOT_HOV' in transformed_df.columns:
                                            file_hov = transformed_df['TOT_HOV'].sum()
                                        elif 'TOT_MOV' in transformed_df.columns:
                                            file_hov = transformed_df['TOT_MOV'].sum() / 60.0

                                        # Prepare Lambda payload for this specific file
                                        lambda_payload = {
                                            'jobType': 'Streamlit',  # Indicates data is already uploaded & normalized
                                            'record_count': record_count,
                                            'tot_hov': round(file_hov, 2),
                                            'platform': platform,
                                            'domain': domain if domain else None,
                                            'filename': filename,
                                            'userEmail': user_email if user_email else None,
                                            'type': data_type if data_type else None,
                                            'territory': territory if territory else None,
                                            'channel': channel if channel else None,
                                            'year': year if year else None,
                                            'quarter': quarter if quarter else None,
                                            'month': month if month else None,
                                            'debug_mode': debug_mode,  # Flag for debug uploads
                                        }

                                        print(f"Lambda event payload for {filename}:", lambda_payload)

                                        # Invoke Lambda for this file
                                        lambda_params = {
                                            'FunctionName': aws_config['lambda_function_name'],
                                            'InvocationType': 'Event',  # Asynchronous invocation
                                            'Payload': json.dumps(lambda_payload)
                                        }

                                        response = lambda_client.invoke(**lambda_params)

                                        if response['StatusCode'] == 202:
                                            lambda_success_count += 1
                                        else:
                                            st.warning(f"Lambda invocation for {filename} returned status: {response['StatusCode']}")

                                    st.success(f"✓ Post-processing triggered for {lambda_success_count}/{len(file_info)} file(s)! You will receive an email when complete.")
                                    st.info("📧 Check your email for processing status (asset matching & final table migration)")

                                except KeyError as e:
                                    st.warning(f"⚠️ AWS configuration missing: {str(e)}. Please configure AWS credentials in secrets.toml")
                                except Exception as e:
                                    st.warning(f"⚠️ Could not trigger post-processing workflow: {str(e)}")
                                    print(f"Lambda invocation error: {str(e)}")
                            else:
                                st.info("ℹ️ Lambda invocation is currently disabled (ENABLE_LAMBDA = False in config.py)")

                            # Add "Upload Another File" button to clear state and start fresh
                            st.divider()
                            col1, col2, col3 = st.columns([1, 2, 1])
                            with col2:
                                if st.button("📤 Upload Another File", type="primary", use_container_width=True):
                                    # Clear the file uploader by clearing session state
                                    # Note: Streamlit file_uploader doesn't have a direct clear method,
                                    # but we can trigger a rerun to reset the widget
                                    st.rerun()

                        except Exception as e:
                            st.error(f"Error loading data: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())

def apply_column_mappings(df, column_mappings, platform, channel, territory, domain, filename=None, year=None, quarter=None, month=None):
    """
    Apply column mappings to transform uploaded data

    Args:
        df: Source dataframe
        column_mappings: Dictionary mapping target columns to source columns
        platform: Platform value to use
        channel: Channel value to use (if provided)
        territory: Territory value to use (if provided)
        domain: Domain value to use (if provided)
        filename: Filename being loaded (optional)
        year: Year value to use (if provided)
        quarter: Quarter value to use (if provided)
        month: Month value to use (if provided)

    Returns:
        Transformed dataframe with standardized column names
    """
    transformed_data = {}

    # Get the number of rows for broadcasting scalar values
    num_rows = len(df)

    # Always use the platform from the parameter
    transformed_data['PLATFORM'] = [platform] * num_rows

    # Add filename if provided
    if filename:
        transformed_data['FILENAME'] = [filename] * num_rows

    # Use channel, territory, and domain if provided (map to correct column names)
    if channel:
        transformed_data['PLATFORM_CHANNEL_NAME'] = [channel] * num_rows
    if territory:
        transformed_data['PLATFORM_TERRITORY'] = [territory] * num_rows
    if domain:
        transformed_data['DOMAIN'] = [domain] * num_rows

    # Add year, quarter, and month if provided
    if year:
        transformed_data['YEAR'] = [year] * num_rows
    if quarter:
        transformed_data['QUARTER'] = [quarter] * num_rows
    if month:
        transformed_data['MONTH'] = [month] * num_rows

    # Define special column name mappings for platform_viewership table
    column_name_mapping = {
        'Partner': 'PLATFORM_PARTNER_NAME',
        'Series': 'PLATFORM_SERIES',
        'Content Name': 'PLATFORM_CONTENT_NAME',
        'Content ID': 'PLATFORM_CONTENT_ID',
        'Channel': 'PLATFORM_CHANNEL_NAME',
        'Territory': 'PLATFORM_TERRITORY',
        'CHANNEL': 'PLATFORM_CHANNEL_NAME',  # For optional column mapping
        'TERRITORY': 'PLATFORM_TERRITORY',   # For optional column mapping
    }

    # Apply mappings from configuration
    for target_col, mapping_value in column_mappings.items():
        if target_col == 'Platform':
            # Skip Platform from mappings, we already set it
            continue

        # Handle both old and new mapping formats
        # Old format: {"Partner": "source_column_name"}
        # New format: {"Partner": {"source_column": "name", "transformation": {...}}}
        # Hardcoded format: {"Territory": {"hardcoded_value": "United States"}}

        source_col = None
        transformation_config = None
        hardcoded_value = None
        unit = 'hours'

        if isinstance(mapping_value, dict):
            # New dict format - check for hardcoded_value first
            if 'hardcoded_value' in mapping_value:
                hardcoded_value = mapping_value['hardcoded_value']
            elif 'source_column' in mapping_value:
                source_col = mapping_value['source_column']
                transformation_config = mapping_value.get('transformation')
                unit = mapping_value.get('unit', 'hours')
        else:
            # Old format (backwards compatible) - simple string mapping
            source_col = mapping_value
            unit = column_mappings.get('_total_watch_time_unit', 'hours')

        # Skip metadata keys
        if target_col == '_total_watch_time_unit':
            continue

        # Skip Channel/Territory if not mapped but provided via dropdown
        if target_col == 'Channel' and not source_col and not hardcoded_value and channel:
            continue
        if target_col == 'Territory' and not source_col and not hardcoded_value and territory:
            continue

        # Handle hardcoded values FIRST
        if hardcoded_value is not None:
            # Apply hardcoded value to all rows
            if target_col in column_name_mapping:
                std_col_name = column_name_mapping[target_col]
            else:
                std_col_name = target_col.upper().replace(' ', '_')

            # Broadcast scalar value to match dataframe length
            transformed_data[std_col_name] = [hardcoded_value] * num_rows
            st.info(f"ℹ️ Using hardcoded value for {target_col}: '{hardcoded_value}'")
            continue  # Skip to next column

        # Process if source column exists
        if source_col and source_col in df.columns:
            # Get source data
            source_data = df[source_col]
            original_data = df[source_col].copy()  # Keep a copy of original

            # Apply transformation if configured
            has_transformation = False
            if transformation_config:
                try:
                    source_data = apply_transformation(source_data, transformation_config)
                    has_transformation = True
                except Exception as e:
                    st.warning(f"Transformation error for {target_col}: {str(e)}")

            # Handle Total Watch Time with unit conversion
            if target_col == 'Total Watch Time':
                # Convert to numeric, removing commas if present
                numeric_data = pd.to_numeric(source_data.astype(str).str.replace(',', ''), errors='coerce')

                if unit == 'minutes':
                    transformed_data['TOT_MOV'] = numeric_data
                else:
                    transformed_data['TOT_HOV'] = numeric_data
            else:
                # Check if this column has a special mapping
                if target_col in column_name_mapping:
                    std_col_name = column_name_mapping[target_col]
                else:
                    # Convert target column name to uppercase with underscores
                    std_col_name = target_col.upper().replace(' ', '_')

                # If transformation was applied, store both original and transformed
                if has_transformation and target_col in ['Channel', 'Partner', 'Territory', 'Content Name']:
                    # Store original in PLATFORM_* column
                    transformed_data[std_col_name] = original_data
                    # Store transformed in new column without PLATFORM_ prefix
                    if target_col == 'Content Name':
                        transformed_data['CONTENT'] = source_data
                    else:
                        transformed_data[target_col.upper()] = source_data
                else:
                    # No transformation, just store as usual
                    transformed_data[std_col_name] = source_data
        elif source_col and source_col != "":
            # Column not found - check if this should be a hardcoded value
            # For Channel, Partner, Territory: treat missing columns as hardcoded values
            if target_col in ['Channel', 'Partner', 'Territory']:
                # Use the value as a constant for all rows
                if target_col in column_name_mapping:
                    std_col_name = column_name_mapping[target_col]
                else:
                    std_col_name = target_col.upper().replace(' ', '_')

                # Broadcast scalar value to match dataframe length
                transformed_data[std_col_name] = [source_col] * num_rows  # Use the "source_col" as constant value
                st.info(f"ℹ️ Using hardcoded value for {target_col}: '{source_col}'")
            else:
                # For other columns, warn that column is missing
                st.warning(f"Column '{source_col}' specified in mapping but not found in uploaded file")

    # Create dataframe from transformed data
    result_df = pd.DataFrame(transformed_data)

    return result_df

if __name__ == "__main__":
    main()
