//********************************************************************************
// Copyright (c) 2013-2013 Intel Corporation. All rights reserved.              **
//                                                                              **
// Redistribution and use in source and binary forms, with or without           **
// modification, are permitted provided that the following conditions are met:  **
//   * Redistributions of source code must retain the above copyright notice,   **
//     this list of conditions and the following disclaimer.                    **
//   * Redistributions in binary form must reproduce the above copyright        **
//     notice, this list of conditions and the following disclaimer in the      **
//     documentation and/or other materials provided with the distribution.     **
//   * Neither the name of Intel Corporation nor the names of its contributors  **
//     may be used to endorse or promote products derived from this software    **
//     without specific prior written permission.                               **
//                                                                              **
// This software is provided by the copyright holders and contributors "as is"  **
// and any express or implied warranties, including, but not limited to, the    **
// implied warranties of merchantability and fitness for a particular purpose   **
// are disclaimed. In no event shall the copyright owner or contributors be     **
// liable for any direct, indirect, incidental, special, exemplary, or          **
// consequential damages (including, but not limited to, procurement of         **
// substitute goods or services; loss of use, data, or profits; or business     **
// interruption) however caused and on any theory of liability, whether in      **
// contract, strict liability, or tort (including negligence or otherwise)      **
// arising in any way out of the use of this software, even if advised of       **
// the possibility of such damage.                                              **
//********************************************************************************

#include <iostream>
#include <fstream>
#include <string>
#include <getopt.h>
#include <stdio.h>
//#include <cnc/debug.h>

#include "pipeline_graph.hpp"
#include "parse_json.hpp"
#include "precipes.hpp"

#include "libjson.h"
#include <curl/curl.h>

#include <boost/filesystem.hpp> // for boost::filesystem::temp_directory_path()

using std::endl;
using std::cin;
using std::cout;
using std::cerr;

/* callback for curl fetch */
size_t curl_callback (void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;                             /* calculate buffer size */
    string* buffer_ptr = static_cast<string*>(userp);
    *buffer_ptr = string( static_cast<char*>(contents), realsize );
    return realsize;
}

enum cwl_request_type {
  CWL_GET_COMMAND_LINE,
  CWL_GET_OUTPUTS
};

const char* request_name(cwl_request_type req) {
  switch ( req ) {
  case CWL_GET_COMMAND_LINE: return "get_command_line";
  case CWL_GET_OUTPUTS: return "get_outputs";
  default:
      cerr << "ERROR invalid cwl_request_type" << endl;
      return "";
  }
}

string cwl_rabix_request(cwl_request_type request_type, const string& request_data_str) {
  CURL *curl;
  CURLcode res;
  
  string buffer;

  /* get a curl handle */ 
  curl = curl_easy_init();
  if(curl) {
    /* First set the URL that is about to receive our POST. This URL can
       just as well be a https:// URL if that is what should receive the
       data. */
    string url="http://localhost:5000/";
    url += request_name(request_type);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    /* Now specify the POST data */ 
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data_str.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);

    /* Perform the request, res will get the return code */ 
    res = curl_easy_perform(curl);
    /* Check for errors */ 
    if(res != CURLE_OK)
      fprintf(stderr, "curl_easy_perform() failed: %s\n",
              curl_easy_strerror(res));
 
    /* always cleanup */ 
    curl_easy_cleanup(curl);
  }
  return buffer;
}


void prompt_user_input( const string& run_name, int run_id, pipeline& pl ) {
  string label, value;
  cout << std::flush;
  cout << "**Enter inputs for " << run_name << ", exit input mode by entering empty line**" << endl;
  while(1) {
    cout << "input label> " << std::flush;
    std::getline(cin, label);
    if ( label.empty() )
      return;
    cout << "input value> " << std::flush;
    std::getline(cin, value);
    pl.input( run_name, run_id, label, value );
  }
}

void display_usage() {
  puts("pipeline - Parallel pipeline script execution");
  puts("  DESCRIPTION: Executes a given pipeline workflow (a json configuration file) for each given sample names. The pipeline will be 'instantiated' for each sample name, running them in parallel.");
  puts("  USAGE: pipeline [options] pipeline_config_file <sample_names>");
  puts("         pipeline -c [options] cwl_workflow <cwl_input_objects>  ## CWL mode");
  puts("  OPTIONS: ");
  puts("		-verbose, -v	: verbose output, prints every command/system call made and prints detailed Intel CnC statistics");
  puts("		-dryrun, -d	: do not execute any stage commands, useful for testing if the pipeline would run properly");
  puts("		-printgraph, -p file 	: do not run, but print the pipeline graph to file in a GraphViz dot format.");
  puts("		-cwl, -c 	: interpret the input pipeline configuration as a cwl workflow, expecting a fully linked and valid json CWL workflow.");
  puts("		-manual, -m 	: interactively requests manual input values during execution from stdin, useful for debugging purposes");
  exit( EXIT_FAILURE );
}

// void print_json_pipeline_graph( const std::string & json_str, std::string filename ) {
//   PLGraph pl_graph;
//   JSONNode root_node = libjson::parse(json_str);
//   for ( auto& n : root_node) {
//     /* if( ( n.name() != "stages" ) ) { */
//     /* 	std::cerr << "expected JSON object with \"stages\", but found " << n.name() << " "  << n.as_string() << std::endl; exit( 1 ); */
//     /* } */
//     if ( n.name() == "stages" )
//       for ( auto& node : n ) {
// 	// nodes called splitjoin are a special case
// 	if ( node.name() == "splitjoin" )
// 	  parse_splitjoin_node( node, pl_graph );
// 	else
// 	  parse_stage_node( node, pl_graph );
//       }
//   }
//   std::ofstream dotfile( filename.c_str(), std::ofstream::out | std::ofstream::trunc );
//   pl_graph.write_graphviz(dotfile);
//   dotfile.close();
// }

// ********************************************************************************************

typedef vector< std::pair<string, string> > bindings_t;

static bindings_t  perform_cwl_command( const string&     cwl_run_string,
					const bindings_t& input_bindings ) {
  LOG( "  performing cwl step with:" );
  LOG( "    run:" + cwl_run_string );
  LOG( "    bindings:");
  for( auto&& binding : input_bindings ) {
    LOG( "       label:" + binding.first + " value: " + binding.second);
  }
  JSONNode request;
  JSONNode tool_cfg = libjson::parse(cwl_run_string);
  tool_cfg.set_name( "tool_cfg" );
  request.push_back( tool_cfg );
  JSONNode input_map;
  for( auto&& binding : input_bindings ) {
    input_map.push_back( JSONNode(binding.first, binding.second) );
  }
  input_map.set_name( "input_map" );

  // CWL requires each application to be executed in a separate and clean temp directory
  //   we use a unique path created by boost
  // TODO: deal with input file objects, afaik. they need to be copied to the temp directory
  auto temp_path = boost::filesystem::temp_directory_path() / boost::filesystem::unique_path();
  string tempdir = temp_path.native();
  assert( boost::filesystem::create_directory(temp_path) );
  request.push_back(input_map);
  request.push_back( JSONNode{ "job_dir", tempdir } );
  LOG("requesting command from rabix using: " << request.write_formatted().c_str());
  string command_answer_str = cwl_rabix_request( CWL_GET_COMMAND_LINE, request.write() );
  LOG("got answer from rabix! \n" << command_answer_str.c_str());
  JSONNode response = libjson::parse(command_answer_str);
  JSONNode arguments = response["arguments"].as_array();
  std::stringstream command;
  for ( auto&& arg : arguments ) {
    command << arg.as_string() << ' ';
  }
  string in_stream  = response["stdin"].as_string();
  string out_stream = response["stdout"].as_string();
  if (  !in_stream.empty() && (in_stream != "null") )
    command << " < " << in_stream;
  if ( !out_stream.empty() && (out_stream != "null") )
    command << " > " << out_stream;
  
  LOG("command string to be executed: " + command.str());
  int exit_code = 0;
  // do a syscall with 'command.str()' here, dryrun for now
  // TODO: before execution, copy input file objects to temp dir of this execution
  //  ** It is possible Rabix will handle this in the future **
  
  // quick hack, re-using the same request and adding the exit_code
  JSONNode output_request( request );
  output_request.push_back( JSONNode{"exit_code",exit_code} );
  string outputs_answer_str = cwl_rabix_request( CWL_GET_OUTPUTS, output_request.write() );
  JSONNode outputs_response = libjson::parse( outputs_answer_str );
  LOG("rabix output response: " + outputs_response.write_formatted());
  string status = outputs_response["status"].as_string();
  JSONNode outputs = outputs_response["outputs"];
  bindings_t output_bindings;
  if ( status == "SUCCESS" ) {
    LOG( "output bindings:" );
    for ( auto&& out_arg : outputs ) {
      LOG( "\t" << out_arg.name() << ": " << out_arg.write() );
      // TODO: append the current operation's name to the output binding
      //       e.g. out_arg.name() is just 'output' while the correct identifier is actually #rev.input
      output_bindings.push_back( std::make_pair( out_arg.name(), out_arg.write() ) );
    }
  }
  else
    std::cerr << "Warning, tool execution response was: " << status << ", ignoring result as only SUCCESS is currently supported.";
  return output_bindings;
}

static int perform_command( const string& command_string,
			    const string& run_name ) {
  string syscall = substitute_run_tag( command_string, run_name );

  if (VERBOSE_FLAG)
    std::cerr << syscall << std::endl;

  int exit_code = 0; // linux non-zero = fail
  if (!DRYRUN_FLAG)
    exit_code = system( syscall.c_str() );

  // does not produce any items if the system command failed
  if (exit_code && VERBOSE_FLAG) {
    std::cerr << "Command failed, not producing any output items." << std::endl <<
      "\tCommand:\t" << syscall << std::endl <<
      "\tExit code:\t" << exit_code << std::endl;
  }
  return exit_code;
}

// returns exit code as well as a string containing the stdout produced by running the command
static pair<int, string> perform_command_capture_out( const string& command_string,
						      const string& run_name ) {
  string tagged_command = substitute_run_tag( command_string, run_name );
  string syscall = tagged_command;
  string command_output;
  int exit_code = 0; // treating non-zero = fail
  
  if (VERBOSE_FLAG)
    std::cerr << syscall << std::endl;
  if (!DRYRUN_FLAG) {
    FILE* fp = popen( syscall.c_str(), "r" );
    char buf[64];
    while (fgets(buf, sizeof buf, fp)) {
      command_output.append(buf);
    }
    exit_code = pclose( fp );
  }
  // does not produce any items if the system command failed
  if (exit_code && VERBOSE_FLAG) {
    std::cerr << "Command failed, not producing any output items." << std::endl <<
      "\tCommand:\t" << syscall << std::endl <<
      "\tExit code:\t" << exit_code << std::endl;
  }
  return std::make_pair(exit_code, command_output);
}

// ********************************************************************************************

int Stage::execute( const run_tag_t & tag, pipeline & pl ) const {
  string run_name = tag.first;
  int run_id = tag.second;
  string item;
  if ( CWL_INPUT ) {
    vector< std::pair<string,string> > input_bindings;
    // a simple get() will restart the step on the first unavailable item
    // we use unsafe get here to do all get()s simulaniously,
    //  which will declare we are dependent on those items
    auto&& input_name_it = m_input_names.begin();
    for( DataCollection& in_dependency : m_in ) {
      in_dependency.unsafe_get( tag, item );
      input_bindings.emplace_back( *input_name_it, item );
      ++input_name_it;
    }
    // declare all inputs to have been requested
    pl.flush_gets();
    LOG("stage execute: " << run_name << ", " << run_id);
    bindings_t outputs = perform_cwl_command( m_syscall,
					      input_bindings );
    for ( auto&& binding : outputs ) {
      // CWL/Rabix has some weird identifier behavior; we are getting an non-global output object id, where normally identifiers are #<stage_name>.<local_name>.
      // quick fix here: append stage name. Take care, because this behavior might change in later Rabix versions (or different CWL implementations)
      string&& output_collection_name = m_name + "." + binding.first;
      string value = binding.second;
    
      // FIXME: output collections are also remembered by this Stage object,
      //   but we need a by-name lookup here, hence the hack of looking up in pl.m_dataColls
      // replace with a per-stage list of names in a second implementation pass
      auto&& out_collection_ptr = pl.m_dataColls[output_collection_name];    
      if (out_collection_ptr)
	out_collection_ptr->put( tag, value );
      else {
	std::cerr <<
	  "WARNING during stage execution (output ignored): CWL output object has an incorrect identifier, "
	  "I do not know any output collections identified by "<< output_collection_name << std::endl;
      }
    }
  }
  else { // precipes mode, not CWL
    // a simple get() will restart the step on the first unavailable item
    // we use unsafe get here to do all get()s simulaniously,
    //  which will declare we are dependent on those items
    for( DataCollection& in_dependency : m_in )
      in_dependency.unsafe_get( tag, item );
    // declare all inputs to have been requested
    pl.flush_gets();
    LOG("stage execute: " << run_name << ", " << run_id);
    if ( perform_command( m_syscall, run_name ) == EXIT_SUCCESS ) 
      // declare output as ready by putting items
      for ( DataCollection& out_dependency : m_out )
	out_dependency.put( tag, item );  
  }
  return CnC::CNC_Success;
}

  int SplitStage::execute( const run_tag_t & tag, pipeline & pl ) const {
    string run_name = tag.first;
    //  int run_id = tag.second;
    //  int item = THE_DUMMY_ITEM;
    string item;
    // a simple get() will restart the step on the first unavailable item
    // we use unsafe get here to do all get()s simulaniously,
    //  which will declare we are dependent on those items
    for ( DataCollection& in_dependency : m_in ) {
      in_dependency.unsafe_get( tag, item );
    }
    // declare all inputs to have been requested
    pl.flush_gets();
    LOG("split execute: " << run_name);
    if ( perform_command( m_syscall, run_name ) == EXIT_SUCCESS ) {
      // due to limited coordination between processes, I can only see two ways of doing the split
      //   1. let the split do _all_ splits, then call a separate 'count' command to tally the #chunks and kick the splitjoin stages for each chunk
      //   2. call the split command until it fails, each call is considered to have produced a chunk and splitjoin stages are kicked for that chunk_array_regex
      // the benefit of 2. is that
      //     - splitjoin stages can overlap while split is still working
      //     - and that you do not need a 'count' command
      // however, 1. seems to fit our current sequencing workflow case better for now (using linux split command on fastq file)
      // so we currently implement option 1.
      for ( DataCollection& out_dependency : m_out )
	out_dependency.put( tag, item );
      m_splitjoin.fanout( tag, pl, m_fanout );
    }
    return CnC::CNC_Success;
  }

int JoinStage::execute( const run_tag_t & tag, pipeline & pl ) const {
  string      run_name = tag.first;
  // int                run_id = tag.second;
  //int             item = THE_DUMMY_ITEM;
  string item;
  for ( DataCollection& in_dependency : m_in ) {
    in_dependency.unsafe_get( tag, item );
  }
  int chunk_count = m_splitjoin.fanin( tag, pl, m_fanin );
  pl.flush_gets();
  LOG("join execute: " << run_name);
  string unrolled_chunk_array = substitute_chunk_array(m_syscall, chunk_count);
  LOG("   chunk array substitution result: " + unrolled_chunk_array);
  if ( perform_command(unrolled_chunk_array, run_name) == EXIT_SUCCESS ) 
    for ( DataCollection& out_dependency : m_out )
      out_dependency.put( tag, item );
  return CnC::CNC_Success;
}

int SplitJoinStage::execute( const pair<int, run_tag_t> & tag, pipeline & pl ) const {
  int           chunk_id = tag.first;
  run_tag_t      run_tag = tag.second;
  string     run_name = run_tag.first;
  //int                  run_id = run_tag.second;
  int               item = THE_DUMMY_ITEM;
  // 'external' (outside this split-join) dependencies
  // for ( DataCollection&        in_dependency : m_in )
  //   in_dependency.unsafe_get( run_tag, item );
  // 'internal' dependencies, notice the tag difference
  for ( ChunkedDataCollection& in_dependency : m_in )
    in_dependency.unsafe_get( tag, item );
  pl.flush_gets();
  LOG("splitjoin stage execute: " << run_name << ", ##=" << chunk_id);

  if ( perform_command( substitute_chunk_tag(m_syscall, chunk_id), run_name ) == EXIT_SUCCESS ) {
    for ( ChunkedDataCollection& out_dependency : m_out )
      out_dependency.put( tag, item );
  }
  return CnC::CNC_Success;
}

// ********************************************************************************************
// TODO: make fanout/fanin really const, find a way to deal with the .put in tag collections, look it up via the pipeline object for instance

void SplitJoin::fanout( const run_tag_t & tag, pipeline& pl, const vector<ChunkedDataCollectionRef>& fanout ) const {
  string run_name = tag.first;
  int item = THE_DUMMY_ITEM;
  SplitJoin* splitjoin = pl.get_splitjoin(m_splitjoin_name);
  assert( splitjoin );  
  if ( m_count_command.empty() ) {
    cerr << "ERROR: 'count' command for " + m_splitjoin_name << " is empty!  Did you forget to specify a count entry in the splitjoin?" << endl;
    return;
  }
  cout<<'\t'<<run_name<<":\tSplitJoin performing fanout"<<endl<<std::flush;
  auto capture = perform_command_capture_out( m_count_command, run_name );  
  if (capture.first == EXIT_SUCCESS) { // test exit code
    int count = std::stoi(capture.second); // TODO: try/catch invalid argument exception for conversion problems
    if ( count < 0 )
      cerr << "WARNING: 'count' command for " + m_splitjoin_name << "returned a negative number" << std::endl;
    for ( int chunk_id=0; chunk_id < count; ++chunk_id ) {
      auto chunk_tag = make_chunk_tag(chunk_id, tag);
      splitjoin->prescribe_chunk( chunk_tag );
      for ( ChunkedDataCollection& col : fanout ) {
        col.put( chunk_tag, item );
      }
    }
    splitjoin->put_count( tag, count );
  }
  else
    std::cerr << "Something went wrong with running the 'count' command on a splitjoin. \n\tSystem call output: "
              << capture.second << std::endl;
}

int SplitJoin::fanin( const run_tag_t& tag, pipeline& pl, const vector<ChunkedDataCollectionRef>& fanin ) const {
  int item = THE_DUMMY_ITEM;
  int chunk_count = -1;
  m_count_collection.get(tag, chunk_count);
  assert( chunk_count >= 0 );
  LOG("fanin count: " << chunk_count);
  for ( chunk_id_t chunk_id=0 ; chunk_id < chunk_count ; ++chunk_id ) {
    auto chunk_tag = std::make_pair(chunk_id, tag);
    for ( ChunkedDataCollection& col : fanin )
      col.unsafe_get( chunk_tag, item );
  }
  return chunk_count;
}

// ********************************************************************************************

void pipeline::serialize( CnC::serializer & ser ) {
  LOG(" Serializing CnC Context");
  // printf("Serializing CnC Context\n");
    // send only the PLGraph object over the wire
    // PLGraph uses boost's serialization functionality
    //  boost's serialization makes all this significantly more painful than it should
    //  I cannot seem to avoid using and copying a byte buffer several times
  if (ser.is_packing()) {
    std::stringstream buf(std::ios::out|std::ios::binary);
    
    boost::archive::binary_oarchive archive_out{buf};
    archive_out << m_graph
		<< m_cwl_configuration;
    // look at and send over the stringstream buffer as a raw byte array
    // size_t size = buf.tellp() - buf.tellg();
    // raw_bytes = new char[size];
    // buf.read( raw_bytes, buf.tellg() );
    auto backing_string = buf.str();
    auto backing_buf = const_cast<char*>(backing_string.c_str());
    auto backing_len = backing_string.size();
    ser & backing_len
        & CnC::chunk<char>{ backing_buf, backing_len };
      //      delete[] raw_bytes;
  }
  else if (ser.is_unpacking()) {
    // receive a raw byte array (alloc managed by CnC)
    char* raw_bytes = NULL;
    size_t len = 0;
    ser & len
        & CnC::chunk<char>{ raw_bytes, len };
    std::stringstream buf(std::string(raw_bytes, len), std::ios::in|std::ios::binary);
    boost::archive::binary_iarchive archive_in{buf};
    archive_in >> m_graph
	       >> m_cwl_configuration;
    m_graph.generate_cnc_graph(*this);
  }
}

// ********************************************************************************************
// ********************************************************************************************



int main( int argc, char * argv [] ) {

#ifdef _DIST_
  CnC::dist_cnc_init< pipeline > _di;
#endif  
  
/* Flags set by getopt options ‘--verbose’ etc. */
    std::string dotgraph_filename;
    VERBOSE_FLAG=0;
    DRYRUN_FLAG=0;
    PRINTGRAPH_FLAG=0;
    LOG_LEVEL=0;
    CWL_INPUT=0;
    MANUAL_INPUT=0;
    while (1) {
      
      static struct option long_options[] =
	{
	  /* These options set a flag. */
	  {"verbose", no_argument, &VERBOSE_FLAG, 1},
	  {"dryrun",  no_argument, &DRYRUN_FLAG, 1},
	  {"cwl",     no_argument, &CWL_INPUT, 1},
	  {"manual",  no_argument, &MANUAL_INPUT, 1},
	  /* These options don’t set a flag.
	     We distinguish them by their indices. */
	  {"printgraph", required_argument, 0, 'p'},
	  {"help", no_argument, 0, 'h'},
	  {0, 0, 0, 0}
	};
      /* getopt_long stores the option index here. */
      int option_index = 0;
      int c = getopt_long (argc, argv, "vdcp:h",
			   long_options, &option_index);

      /* Detect the end of the options. */
      if (c == -1)
	break;

      switch (c) {
      case 0:
	/* If this option set a flag, do nothing else now. */
	if (long_options[option_index].flag != 0)
	  break;
	printf ("option %s", long_options[option_index].name);
	if (optarg)
	  printf (" with arg %s", optarg);
	printf ("\n");
	break;

      case 'c':
	CWL_INPUT=1;
	break;

      case 'm':
	MANUAL_INPUT=1;
	break;
	
      case 'v':
	VERBOSE_FLAG=1;
	LOG_LEVEL=2;
	break;

      case 'd':
	DRYRUN_FLAG=1;
	break;

      case 'p':
	PRINTGRAPH_FLAG=1;
	dotgraph_filename=std::string(optarg);
	break;

      case 'h':
      case '?':
	display_usage();
	break;

      default:
	abort ();
      }
    }
    
    /* Instead of reporting ‘--verbose’
       and ‘--brief’ as they are encountered,
       we report the final status resulting from them. */
    if (VERBOSE_FLAG) {
      puts ("verbose flag is set");
    }
    if (DRYRUN_FLAG) {
	puts("Doing a dryrun of the pipeline without actually executing any stages.\n");
	puts(" Warning: this assumes all stages succeed, which might not always be what you want.\n");
    }

    // argument list indices and counts for precipes
    const int config_arg_idx = optind;
    const int restargc       = argc - optind + 1;
    const int runs_idx       = optind + 1;
    const int run_argc       = argc - runs_idx;
    
    if( restargc <= 1 ) {
      std::cout << "No config file specified.\n";
      display_usage();
    }
    
    std::ifstream pipeline_file(argv[config_arg_idx]);
    if ( !pipeline_file.good() ) {
      std::cerr << "ERROR: cannot open configuration file \"" << argv[config_arg_idx] << "\"" << std::endl;
      exit(EXIT_FAILURE);
    }
    std::string json_str( std::istreambuf_iterator<char>{pipeline_file},
			  std::istreambuf_iterator<char>{} );
    pipeline_file.close();

    if ( json_str.empty() ) {
      std::cerr << "ERROR: empty configuration file \""  << argv[config_arg_idx] << "\"" << std::endl;
      exit(EXIT_FAILURE);
    }

    
    PLGraph pl_graph = PLGraph{}; // this graph will be owned by the pipeline object further down

    if ( CWL_INPUT ) 
      parse_cwl_workflow( json_str, pl_graph );
    else
      parse_json_pipeline_graph( json_str, pl_graph );
    
    if ( PRINTGRAPH_FLAG ) {
      std::ofstream dotfile( dotgraph_filename.c_str(), std::ofstream::out | std::ofstream::trunc );
      pl_graph.write_graphviz(dotfile);
      dotfile.close();
      std::cout << "Written GraphViz dot graph to \"" << dotgraph_filename << "\"" << std::endl;
      return 0;
    }

    if( run_argc <= 0 ) {
      if ( CWL_INPUT )
	std::cerr << "ERROR: No CWL arguments (or input objects) specified.\n";
      else
	std::cerr << "ERROR: No runs specified.\n";
      display_usage();
      exit(EXIT_FAILURE);
    }
    
    /** CnC context gets initialized here */
    pipeline cnc_pipeline{pl_graph};

    if ( CWL_INPUT )
      cnc_pipeline.set_cwl_configuration( json_str );
    
    if ( VERBOSE_FLAG ) {
      CnC::debug::trace_all( cnc_pipeline );
      CnC::debug::collect_scheduler_statistics( cnc_pipeline );
      CnC::debug::init_timer();
    }

    // read run names from argv
    vector<char*> run_args(run_argc);
    for( int run_id = 0; run_id < run_argc; ++run_id ) {
      run_args[run_id] = argv[runs_idx + run_id];
    }

    // We currently talk to Rabix via a libcurl interface to do CWL processing stuff at runtime
    //  curl needs a a global init and cleanup, which is best put here
    curl_global_init(CURL_GLOBAL_ALL);

    // process runs (in parallel)
    if ( CWL_INPUT ) {
      vector<string> input_arg_ids = parse_cwl_input_schema( json_str );
      printf("Running CWL workflow in batched command line mode.\n"
	     "The arguments to this command line invocation (after the CWL workflow specification file)\n"
	     "are interpreted as input values for the given CWL workflow.\n");
      if ( input_arg_ids.empty() ) {
	cerr << "Your workflow has no inputs and will start executing immediately in batch mode, the specified arguments to this command line invocation are taken as run names (batch names). If this is not correct due to some error, it is possible the precipes engine will keep waiting for input; press Ctrl+C to abort." << endl;
	for( int run_id = 0; run_id < run_argc; ++run_id ) {
	  cnc_pipeline.run( run_args[run_id], run_id );
	}
	cnc_pipeline.wait();
      }
      else {
	int num_args = input_arg_ids.size();
	int num_runs = run_argc / num_args;
	if ( run_argc % num_args )
	  cerr << "Warning: insufficient arguments (after CWL workflow specification file), expecting a multiple of " << num_args << endl;
	cout << "Based on the given workflow and command line arguments, precipes is starting execution of " << num_runs <<
	  " runs in batch mode, with the following run names and input bindings: " << endl;

	int arg_id = 0;
	for ( int run_id = 0; run_id < num_runs; run_id++ ) {
	  printf(" * Batch #%d\n", run_id);
	  cnc_pipeline.run( std::to_string(run_id), run_id );
	  for ( string& arg_name : input_arg_ids ) {
	    string arg_value = run_args[arg_id++];
	    cout << "   - \"" << arg_name << "\" : " << arg_value << endl;
	    cnc_pipeline.input( std::to_string(run_id) , run_id, arg_name, arg_value );
	  }
	}

	// TODO: handle CWL output schema
	//  - Do a get() with the id of the output elements as label. This can be a safe get(), in which case you can even not do the cnc_pipeline.wait(). This can also be an unsafe_get() or similar unsafe operations (e.g. iterate over collections) as long as this happens after the cnc_pipeline.wait() line below.
	//  - Create a json output object from the collected output elements above, either to cout or file (maybe make it a command line option?)
	cnc_pipeline.wait();
      }
    }
    else {
      for( int run_id = 0; run_id < run_argc; ++run_id ) {
	cnc_pipeline.run( run_args[run_id], run_id );
	if ( MANUAL_INPUT )
	  prompt_user_input( run_args[run_id], run_id, cnc_pipeline);
      }
      cnc_pipeline.wait();	  
    }
    // NOTE: .wait() is a blocking wait untill CnC reaches a quiescent state
    //        TBB_TASK (default) scheduler will re-use the thread that does the wait() (viz. this one) as a worker
    //        This will result in CNC_NUM_THREADS+1 worker threads, which might screw up your scaling benchmarks results
    //        Workaround: set CNC_SCHEDULER to something else like TASK_STEAL, or do not use wait() (see below)

    // alternative to wait() is to get() the desired output elements. precipes does not have dedicated output nodes, but it could be derived from the graph shape
    //   NB. a CWL workflow description _does_ directly name the workflow output with the "outputs" element


    if (VERBOSE_FLAG) {
      CnC::debug::finalize_timer(NULL);
    }

    curl_global_cleanup();

    return 0;
}
