#pragma once

#include <boost/regex.hpp>
#include <iostream>

// TODO: replace boost::regex with std::regex when guaranteed to compile on gcc4.9+, std should have exactly the same interface

const boost::regex run_tag_regex("{}");
const boost::regex chunk_tag_regex("##");
const boost::regex chunk_array_regex("@\\((.*)\\)");

std::string substitute_run_tag( const std::string& command_string, const std::string& run_tag ) {
  return boost::regex_replace( command_string, run_tag_regex, run_tag.c_str() );
}

std::string substitute_chunk_tag( const std::string& command_string, int chunk_id ) {
  return boost::regex_replace( command_string, chunk_tag_regex, std::to_string(chunk_id).c_str());
}

std::string unroll_chunks( const std::string& command_string, int num_chunks ) {
  std::string unrolled;
  auto it = std::back_inserter(unrolled);
  for (int i=0; i<num_chunks; ++i) {
	boost::regex_replace(it, command_string.begin(), command_string.end(), chunk_tag_regex, std::to_string(i).c_str() );
	it = ' ';
  }
  return unrolled;
}

// extracts a single chunk array @(...) from the command string
std::string extract_chunk_array( const std::string& command_string ) {
  boost::smatch match;
  if (boost::regex_search( command_string, match, chunk_array_regex ))
	return std::string(match[1].first, match[1].second);
  else
	return "";
}

std::string substitute_chunk_array( const std::string& command_string, int num_chunks ) {
  assert( num_chunks > 0 );
  std::string extracted_array      = extract_chunk_array( command_string );
  std::string unrolled_chunked_str = unroll_chunks( extracted_array, num_chunks );
  return boost::regex_replace( command_string, chunk_array_regex, unrolled_chunked_str );  
}

// TODO: insert result checks, returns false for now;
bool test_regex(const std::string str) {
  // example: "$SAMTOOLS merge -f $LOCAL_DIR/{}.unpaired.bam @($LOCAL_DIR/{}.chunk_##.sorted.unpaired.bam) "
  std::string tagged_str           = substitute_run_tag( str, "SAMPLE_01" );
  std::string tagged_chunked_str   = substitute_chunk_tag( tagged_str, 1 );
  std::string extracted_array      = extract_chunk_array( tagged_str );
  std::string unrolled_chunked_str = unroll_chunks( extracted_array, 4 );
  std::cout << "input :  "         << str                << std::endl;
  std::cout << "tagged : "         << tagged_str         << std::endl;
  std::cout << "tagged chunked : " << tagged_chunked_str << std::endl;
  std::cout << "chunk array : "    << extracted_array    << std::endl;
  std::cout << "4x unrolled tagged chunked : " << unrolled_chunked_str << std::endl;
  return false;
}
