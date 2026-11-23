# Applies PATCH_FILE cleanly in the current directory, succeeding if it has
# already been applied in a previous configure run.
if(NOT DEFINED PATCH_FILE)
    message(FATAL_ERROR "PATCH_FILE is required")
endif()

execute_process(
    COMMAND git apply --reverse --check "${PATCH_FILE}"
    RESULT_VARIABLE already_applied
    OUTPUT_QUIET
    ERROR_QUIET
)
if(already_applied EQUAL 0)
    return()
endif()

execute_process(
    COMMAND git apply "${PATCH_FILE}"
    RESULT_VARIABLE apply_result
)
if(NOT apply_result EQUAL 0)
    message(FATAL_ERROR "Failed to apply patch ${PATCH_FILE}")
endif()
