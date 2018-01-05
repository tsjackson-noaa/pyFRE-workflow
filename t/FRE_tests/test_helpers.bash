# add load test_helpers to the top of your bats files to load these functions

# Set default_platform variable
#
# ncrc needs to distinguish between ncrc3 and ncrc4 (for now) when
# testing.  Cannot use FRE_SYSTEM_SITE as on Gaea, it is now "ncrc"
# (no [34]).

if [ "${FRE_SYSTEM_SITE}" = "ncrc" ]
then
   case "$(hostname)" in
      gaea9|gaea1[0-2] )
         default_platform="ncrc3.intel"
         ;;
      * )
         default_platform="ncrc4.intel"
         ;;
   esac
else
   default_platform="${FRE_SYSTEM_SITE}.intel"
fi

unique_stdout_xml() {
# NOTE: Do not use
# I have to transition away from this one
    awk </dev/null '/platform/{s=0}/"'"${FRE_SYSTEM_SITE}"'.intel"/{s=1}{if (s) {if ($0 ~ /<directory/){ret=system("sed s/UNIQUE_STRING/FRE_tests-'"${unique_string}"'-temp/ '"${platform}"'.dirs"); if(ret){exit ret}} else {print}} else {print}}' "$@"
}

unique_dir_xml() {
    awk </dev/null '/platform/{s=0}/"'"${default_platform}"'"/{s=1}{if (s) {if ($0 ~ /<directory/){ret=system("sed s/UNIQUE_STRING/'"${unique_string}"'/ '"${platform}"'.dirs"); if(ret){exit ret}} else {print}} else {print}}' "$@"
}

string_matches_pattern() {
   # string pattern
   [ $( expr "$1" : "$2" ) -gt 0 ]
}

remove_output_matching() {
    output=$(printf '%s' "$output" | grep -v "$1")
}

remove_lines_matching() {
    for k in "${!lines[@]}"
    do
        if string_matches_pattern "${lines[$k]}" "$1"
        then
            unset lines[$k]
        fi
    done
}

remove_ninac_from_output_and_lines() {
    local ninac="NiNaC'ing ...Dir([.]*done\!)"
    remove_lines_matching "$ninac"
    remove_output_matching "$ninac"
}
