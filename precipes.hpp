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

#ifndef _PIPELINE_INCLUDED_
#define _PIPELINE_INCLUDED_

#include <stdio.h>     // for popen and system
#include <iostream>
#include <iterator>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cassert>

// ********************************************************************************************

#define _DIST_

#ifdef _DIST_
#include <cnc/dist_cnc.h>
#else
#include <cnc/cnc.h>
#endif
#include <cnc/debug.h>

// ********************************************************************************************

#include "pipeline_graph.hpp"
#include "command_string.hpp"


// ********************************************************************************************
//  GLOBAL BEHAVIOR FLAGS
int VERBOSE_FLAG, DRYRUN_FLAG, PRINTGRAPH_FLAG;
int LOG_LEVEL;
int CWL_INPUT, MANUAL_INPUT;

// ********************************************************************************************
// ********************************************************************************************

using std::string;
using std::vector;
using std::map;
using std::pair;
using std::endl;
using std::cerr;
using std::cout;

class Stage;
class SplitJoinStage;
class SplitStage;
class JoinStage;

struct stage_tuner;

typedef string run_name_t;
typedef int    run_id_t;
typedef int    chunk_id_t;
typedef pair<run_name_t, run_id_t > run_tag_t;
typedef pair<chunk_id_t, run_tag_t> chunked_tag_t;
typedef vector<string> names_t;

chunked_tag_t make_chunk_tag(chunk_id_t chunk, run_tag_t run) { return std::pair<chunk_id_t, run_tag_t>( chunk, run ); }

typedef CnC::step_collection<         Stage,	stage_tuner> 	         StageColl;
typedef CnC::step_collection<SplitJoinStage, 	stage_tuner> 	SplitJoinStageColl;
typedef CnC::step_collection<    SplitStage, 	stage_tuner> 	    SplitStageColl;
typedef CnC::step_collection<     JoinStage, 	stage_tuner> 	     JoinStageColl;

typedef CnC::item_collection<run_tag_t,     string> DataCollection;
typedef CnC::item_collection<chunked_tag_t, int> ChunkedDataCollection;
typedef std::reference_wrapper<DataCollection> DataCollectionRef;
typedef std::reference_wrapper<ChunkedDataCollection> ChunkedDataCollectionRef;
typedef std::shared_ptr<DataCollection> DataCollectionPtr;
typedef std::shared_ptr<ChunkedDataCollection> ChunkedDataCollectionPtr;  

class pipeline;

int THE_DUMMY_ITEM=-1;

// ********************************************************************************************
// Utility
// ********************************************************************************************

#define LOG(label) if ( LOG_LEVEL > 0 ) { cout << "[LOG] " << label << endl << std::flush; }

// ********************************************************************************************
// ********************************************************************************************

struct stage_tuner : public CnC::step_tuner<> {
  template< typename PL >
  int compute_on( const run_tag_t & tag, PL & pl ) const {
    LOG("computing (" << tag.first << "," << tag.second << ") on node #" << tag.second % numProcs());
    return tag.second % numProcs();
  }
  template< typename PL >
  int compute_on( const chunked_tag_t & tag, PL & pl ) const {
    return CnC::COMPUTE_ON_LOCAL;
  }

};

// ********************************************************************************************

// A generic stage of a pipeline
// Parametrized by command string and input- and output-collections
// create instance with desired parameters and pass it to constructor of step-collection
class Stage //: public CnC::step_tuner<>
{
public:
  Stage( const string & syscall,
         const vector<DataCollectionRef>& ins,
         const vector<DataCollectionRef>& outs,
	 const names_t& input_names,
	 const names_t& output_names) : Stage("Unkown Stage", syscall, ins, outs, input_names, output_names) {}
  Stage( const string & syscall,
         const vector<DataCollectionRef>& ins,
         const vector<DataCollectionRef>& outs) : m_name("Unknown Stage"), m_syscall(syscall), m_in(ins), m_out(outs) {}
  Stage() {}
  Stage( const string & name,
	 const string & syscall,
         const vector<DataCollectionRef>& ins,
         const vector<DataCollectionRef>& outs,
	 const names_t& input_names,
	 const names_t& output_names) : m_name(name), m_syscall(syscall), m_in(ins), m_out(outs), m_input_names(input_names), m_output_names(output_names) {}


  // wire the given step-collection with the prod/cons dependences of this step
  void wire( StageColl & sc ) const
  {
    for ( DataCollection& dep : m_in )
      sc.consumes( dep );
    for ( DataCollection& dep : m_out )
      sc.produces( dep );
  }

  bool empty() const { return m_syscall.empty() && m_in.empty() && m_out.empty(); }

  // note: this must be const, you cannot keep state/mutate "this"
  int execute( const run_tag_t & tag, pipeline & pl ) const;

  /* template< class dependency_consumer, class CTX > */
  /* void depends( const run_tag_t & file_thunk, CTX & arg, dependency_consumer & dC ) const { */
  /*   //(void)arg; */
  /*   //    for( auto i = m_in.begin(); i != m_in.end(); ++i ) */
  /*   //         dC.depends( **i, file_thunk );     */
  /* } */

protected:
  const std::string  m_syscall, m_name;
  const std::vector<DataCollectionRef> m_in, m_out;
  const names_t m_input_names, m_output_names;
};

// ********************************************************************************************

// a splitjoin stage is significantly different from stage:
//  - it is prescribed by a chunked_tag_t rather than run_tag_t
//  - its in and out dependencies are to chunked collections (accessed with chunked_tag_t)
//  - cannot have any dependencies outside the (chunked) data collections of its local splitjoin subgraph
class SplitJoinStage {
  const string m_name;
  const string m_syscall;
  const vector< ChunkedDataCollectionRef > m_in, m_out;
public:
  SplitJoinStage( const string& name,
		  const string& syscall,
                  const vector<ChunkedDataCollectionRef>& in,
                  const vector<ChunkedDataCollectionRef>& out ) : m_name(name), m_syscall(syscall),
								  m_in(in), m_out(out) {}
  
  int execute( const chunked_tag_t& tag, pipeline & pl ) const;

  void wire( SplitJoinStageColl & sc ) const
  {
    for ( ChunkedDataCollection& dep : m_in )
      sc.consumes( dep );
    for ( ChunkedDataCollection& dep : m_out )
      sc.produces( dep );
  }

  const string& name() const { return m_name; };
};

typedef std::shared_ptr<SplitJoinStage> SplitJoinStagePtr;

// ********************************************************************************************

class SplitJoin;

class SplitStage : public Stage {
  const SplitJoin & m_splitjoin;
  const vector<ChunkedDataCollectionRef> m_fanout;
public:
  SplitStage( const Stage& stage,
              const SplitJoin & splitjoin,
              const vector<ChunkedDataCollectionRef>& fanout ) : Stage(stage),
								 m_splitjoin(splitjoin),
								 m_fanout(fanout) {}
  int execute( const run_tag_t & tag, pipeline & pl ) const;
  
  void wire( SplitStageColl & sc ) const 
  {
    Stage::wire( reinterpret_cast<StageColl&>(sc) );
    for ( ChunkedDataCollection& dep : m_fanout ) {
      sc.produces( dep );
    }
  }
};

// ********************************************************************************************

class JoinStage : public Stage {
  const SplitJoin & m_splitjoin;
  const vector<ChunkedDataCollectionRef> m_fanin;

public:
  JoinStage( const Stage& stage,
             const SplitJoin & splitjoin,
             const vector<ChunkedDataCollectionRef>& fanin ) : Stage(stage),
							       m_splitjoin(splitjoin),
							       m_fanin(fanin) {}

  int execute( const run_tag_t & tag, pipeline & pl ) const;

  void wire( JoinStageColl & sc ) const 
  {
    Stage::wire( reinterpret_cast<StageColl&>(sc) );
    for ( ChunkedDataCollection& dep : m_fanin ) {
      sc.consumes( dep );
    }
  }
};

// ********************************************************************************************
// ********************************************************************************************

// This class is used to incrementally build a splitjoin graph, the add_* commands hold the graph information and only on the build() command will an rvalue SplitJoin instance be created.
//  This is necessary to deal with several const-properties + circular dependencies. An added benefit is a separation of concerns: only build() contains cnc-specific code.

// BUILD PROCESS FOR A SPLITJOIN:
// 1. construct SplitJoinBuilder object with splitjoin name
// 2 call add_* functions on builder object to fill in necessary elements: split, join, stage and count
//    during the add_ functions:
// 2.1  data collection names are linked to actual data collection object (references)
// 2.2  Stage (and SplitJoinStage) objects are constructed; these do not contain a reference to a SplitJoin
// 3. construct SplitJoin object by passing all necessary information from the builder
// 3.1  as part of the constructor, create the SplitStage and JoinStage objects, these are const objects that contain a const reference to a SplitJoin object
// 3.2  create the CnC step objects and wire/prescribe (consume/produce/prescribe) to the data/tag collections
class SplitJoinBuilder {
  typedef std::shared_ptr<Stage> StagePtr;
  StagePtr m_split;
  StagePtr m_join;
  string m_count;
  vector<SplitJoinStage> 		m_stages;
  vector<ChunkedDataCollectionRef> 	m_fanin, m_fanout;
  string                		m_splitjoin_name;
  map<string, ChunkedDataCollectionPtr> m_data_collections;
  pipeline&             		m_context;

  // Constructor is private: use add_builder instead!
  SplitJoinBuilder( const string& name, pipeline& pl ) : m_splitjoin_name( name ), m_context(pl) {}
  
  bool valid_build_p() {
    return ! m_count.empty() && m_split && m_join;
  }
  
public:
  void add_count( const string& count_command ) {
    m_count = count_command;
  }
  void add_split( const Stage&   stage,
		  const names_t& fanout_names ) {
    m_split  = std::make_shared<Stage>(stage);
    m_fanout = ensure_data_collections( fanout_names );
  }
  void add_join( const Stage&   stage,
		 const names_t& fanin_names ) {
    m_join  = std::make_shared<Stage>(stage);
    m_fanin = ensure_data_collections( fanin_names );
  }
  // void add_split( const string& 		   stage_command,
  // 		  const vector<DataCollectionRef>& ins,
  // 		  const vector<DataCollectionRef>& outs,
  // 		  const names_t&		   fanout_names ) {
  //   m_split  = Stage{stage_command, ins, outs};
  //   m_fanout = ensure_data_collections( fanout_names );
  // }
  // void add_join( const string& 		   	  stage_command,
  // 		 const vector<DataCollectionRef>& ins,
  // 		 const vector<DataCollectionRef>& outs,
  // 		 const names_t& 		  fanin_names ) {
  //   m_join  = Stage{stage_command, ins, outs};
  //   m_fanin = ensure_data_collections( fanin_names );
  // }
  void add_stage( const string& stage_name, const string& stage_command, const names_t& consuming, const names_t& producing  ) {
      // note that the function ensure_data_collection here for SplitJoin is not the same as the one in the pipeline class
    vector<ChunkedDataCollectionRef> consuming_refs = ensure_data_collections( consuming );
    vector<ChunkedDataCollectionRef> producing_refs = ensure_data_collections( producing );
    m_stages.emplace_back( SplitJoinStage{stage_name, stage_command, consuming_refs, producing_refs} );
  }

  SplitJoin * build();

  /** class-level functions managing lifetimes and allocation of builder instances */
  static SplitJoinBuilder&    add_builder( const string& splitjoin_name, pipeline& pl );
  static SplitJoinBuilder&    get_builder( const string& splitjoin_name );
  static void              remove_builder( const string& splitjoin_name );
  
private:
  // manages the creation and retrieval of data collections inside the splitjoin
  //  operates on different tags than the global pipeline context
  static map<string, SplitJoinBuilder> builder_instances;
  vector<ChunkedDataCollectionRef> ensure_data_collections( const names_t& collection_names );
};

// ********************************************************************************************
// ********************************************************************************************
// ********************************************************************************************


class SplitJoin {
  // execution (CnC) context
  CnC::tag_collection<run_tag_t>      & m_controlling_tags;
  pipeline                            & m_context;
  const string                          m_splitjoin_name;
  const string                          m_count_command;
  // tag collection
  CnC::tag_collection<chunked_tag_t>    m_tags;
  // item collections
  map<string, ChunkedDataCollectionPtr>	m_data_collections;
  CnC::item_collection<run_tag_t, int>  m_count_collection;
  // step collections
  typedef std::shared_ptr<SplitJoinStageColl> SplitJoinStageCollPtr;
  vector<SplitJoinStageCollPtr>      	m_stage_steps;
  SplitStage				m_split;
  JoinStage				m_join;
  SplitStageColl			m_split_step;
  JoinStageColl				m_join_step;

public:
  SplitJoin( const	string& 				splitjoin_name,
	     		pipeline&				pl,
	     		map<string, ChunkedDataCollectionPtr>	data_collections,
	     const 	vector<SplitJoinStage>& 		stages,
	     const 	string&	    				count_command,
	     const 	Stage&			    		join,
	     const 	Stage&			    		split,
	     const	vector<ChunkedDataCollectionRef>	fanin,
	     const	vector<ChunkedDataCollectionRef>	fanout );

  // fanout puts the chunks on the fanout edges/dependencies and sets the chunk_count
  void fanout( const run_tag_t & tag, pipeline& pl, const vector<ChunkedDataCollectionRef>& fanout ) const;
  // fanin gets the chunk_count number of chunks (and this will not continue if not all are read)
  //  returns the chunk_count
  int  fanin(  const run_tag_t & tag, pipeline& pl, const vector<ChunkedDataCollectionRef>& fanin  ) const;

  void prescribe_chunk( chunked_tag_t chunk_tag ) {
    m_tags.put( chunk_tag );
  }
  void put_count( run_tag_t tag, int count ) {
    m_count_collection.put(tag, count);
  }
  
  const string& splitjoin_name() const { return m_splitjoin_name; }
};

// A full pipeline
// populate with stages using add_stage
// splitjoin stages/subgraph usage is more difficult
// to actually start processing, call run once for each run instance (e.g. for each sample)
class pipeline : public CnC::context< pipeline >
{
  friend class SplitJoin;
  friend class Stage;
  typedef std::shared_ptr<SplitJoin>       SplitJoinPtr;
  typedef std::shared_ptr<StageColl>       StageCollPtr;
  typedef map< string, DataCollectionPtr > data_coll_map_type;
  typedef vector< SplitJoinPtr >           splitjoin_coll_type;
  typedef vector< StageCollPtr >           stage_coll_type;
  //  typedef vector< CnC::tag_collection<run_tag_t> >     tag_coll_type;
  data_coll_map_type              m_dataColls;
  splitjoin_coll_type             m_splitjoins;
  stage_coll_type                 m_stages;
  CnC::tag_collection<run_tag_t>  m_tags;
  PLGraph                         m_graph;
  
public:
  string                          m_cwl_configuration;

  pipeline( const PLGraph& graph = PLGraph{} ): m_graph(graph), m_tags(*this) {
    LOG("pipeline constructor: generating cnc graph");
    m_graph.generate_cnc_graph(*this);
  }
  // pipeline( PLGraph& )=delete;
  // pipeline( PLGraph&& graph ): m_graph(std::move(graph)), m_tags(*this) {
  //   m_graph.generate_cnc_graph(*this);
  // }
  //  pipeline(): pipeline( PLGraph{} ) {}
  //pipeline(): m_tags(*this) { printf("EMPTY CTOR\n"); }

  pipeline(const pipeline& pl) = delete;

  void set_cwl_configuration( string json_configuration ) {
    m_cwl_configuration = json_configuration;
  }
  
  /** precipes graph construction interface elements */
  void add_stage( const string& stage_name,
                  const string& stage_command,
                  const names_t& consuming_names,
                  const names_t& producing_names,
		  const names_t& input_ports,
		  const names_t& output_ports ) {
    LOG("add_stage: " + stage_name);
    // makes_stage will map consuming/producing names to actual data collections (viz. CnC Item collections)
    Stage stage_obj = Stage{stage_name,
			    stage_command,
			    ensure_data_collections(consuming_names),
			    ensure_data_collections(producing_names),
			    input_ports,
			    output_ports};    
    StageCollPtr step_ptr = std::make_shared<StageColl>( *this,
							 stage_name,
							 stage_obj,
							 stage_tuner{} );
    stage_obj.wire( *step_ptr );
    m_tags.prescribes( *step_ptr, *this );
    m_stages.push_back( step_ptr );
    if (VERBOSE_FLAG) {
      CnC::debug::trace(*step_ptr);
      CnC::debug::time(*step_ptr);
    }
  }

  // splitjoin construction operations
  //  usage:  begin_splitjoin( name )
  //          ... various splitjoin_add_X( name, ... ), where X in {count, split, join, stage}
  //          end_splitjoin( name )
  // split, join and count stages are mandatory
  // the split-phase construction is to produce a purely const (immutable) splitjoin object in presence of circular dependencies (e.g. split needs splitjoin, splitjoin needs split)
  
  void begin_splitjoin( const string& splitjoin_name ) {
    LOG("begin_splitjoin: " + splitjoin_name);
    SplitJoinBuilder::add_builder( splitjoin_name, *this );
  }
  
  void splitjoin_add_count( const string& splitjoin_name,
                            const string& count_command ) {
    LOG("add_count: " + splitjoin_name);
    SplitJoinBuilder& builder = SplitJoinBuilder::get_builder( splitjoin_name );
    builder.add_count( count_command );
  }               

  void splitjoin_add_split( const string& splitjoin_name,
                            const string& split_command,
                            const names_t& consuming_names,
                            const names_t& producing_names,
                            const names_t& fanout_names ) {    
    LOG("add_split: "+splitjoin_name);
    SplitJoinBuilder& builder = SplitJoinBuilder::get_builder( splitjoin_name );
    builder.add_split( Stage{split_command, ensure_data_collections(consuming_names), ensure_data_collections(producing_names)},
                       fanout_names );
  }
  
  void splitjoin_add_join( const string& splitjoin_name,
                           const string& join_command,
                           const names_t& consuming_names,
                           const names_t& producing_names,
                           const names_t& fanin_names ) {
    LOG("add_join: "+splitjoin_name);
    SplitJoinBuilder& builder = SplitJoinBuilder::get_builder( splitjoin_name );
    builder.add_join( Stage{join_command, ensure_data_collections(consuming_names), ensure_data_collections(producing_names)},
                      fanin_names );
  }
  
  void splitjoin_add_stage( const string& splitjoin_name,
                            const string& stage_name,
                            const string& stage_command,
                            const names_t& consuming_names,
                            const names_t& producing_names ) {
    LOG("splitjoin_add_stage: " + splitjoin_name + " " + stage_name);
    SplitJoinBuilder& builder = SplitJoinBuilder::get_builder( splitjoin_name );
    builder.add_stage( stage_name, stage_command, consuming_names, producing_names );
  }

  void finish_splitjoin( const string& splitjoin_name ) {
    LOG("finish_splitjoin: "+splitjoin_name);
    SplitJoinBuilder& builder = SplitJoinBuilder::get_builder( splitjoin_name );
    m_splitjoins.push_back( std::shared_ptr<SplitJoin>(builder.build()) );
  }
  
  /*****************************************************/

  // start processing a given data set (name)
  void run( const string & run_name, int run_id ) {
    m_tags.put( run_tag_t{run_name, run_id} );
  }

  // used for 'externally' injecting items in item collections, for instance from manual user input or input object
  //  looks up item collection by its label
  void input( const string& run_name, int run_id, const string & label, const string& value ) {
    auto entry_it = m_dataColls.find( label );
    if ( entry_it == m_dataColls.end() ) {
      cerr << "ERROR: could not found dependency with name " << label << endl;
    }
    else {
      DataCollectionPtr item_collection_ptr = entry_it->second;
      item_collection_ptr->put( run_tag_t{run_name, run_id},
				value );
				
    }
  }

  // used by the steps' execute() const function to fetch a live splitjoin object, without breaking const rules
  SplitJoin* get_splitjoin( const string& splitjoin_name ) {
    auto it = std::find_if( m_splitjoins.begin(), m_splitjoins.end(),
			    [&splitjoin_name](const SplitJoinPtr sj_ptr) {
			      return splitjoin_name == sj_ptr->splitjoin_name(); } );
    return it == m_splitjoins.end() ? NULL : it->get();
  }
			       

#ifdef _DIST_
  virtual void serialize( CnC::serializer & ser ); 
#endif

private:
  /** helper functions ************************************************************************/
  // returns a DataCollection object unique to the given name
  vector<DataCollectionRef> ensure_data_collections( const names_t& names ) {
    vector<DataCollectionRef> data_collections;
    for ( const string& name : names ) {
      auto entry_it = m_dataColls.find( name );
      if ( entry_it == m_dataColls.end() ) {
	auto result_pair = m_dataColls.emplace( name,
						std::make_shared<DataCollection>(*this, name) );
	entry_it = result_pair.first;
      }
      assert(entry_it!= m_dataColls.end());
      DataCollection& col = *(entry_it->second);
      data_collections.push_back( col );
      if ( VERBOSE_FLAG ) {
		CnC::debug::trace(col);
      }
    }
    return data_collections;
  }
};

// ********************************************************************************************
// ********************************************************************************************

vector<ChunkedDataCollectionRef> SplitJoinBuilder::ensure_data_collections( const names_t& collection_names ) {
  vector<ChunkedDataCollectionRef> data_collections;
  for ( const string& name : collection_names ) {
    auto entry_it = m_data_collections.find( name );
    if ( entry_it == m_data_collections.end() ) {
      auto result_pair = m_data_collections.emplace( name,
						     std::make_shared<ChunkedDataCollection>(m_context, name) );
      entry_it = result_pair.first;
    }
    assert( entry_it != m_data_collections.end() );
    data_collections.push_back( *(entry_it->second) );
    if ( VERBOSE_FLAG ) {
      CnC::debug::trace( *(entry_it->second) );
    }

  }
  return data_collections;
}

map<string, SplitJoinBuilder> SplitJoinBuilder::builder_instances;

SplitJoinBuilder& SplitJoinBuilder::add_builder( const string& splitjoin_name, pipeline& pl ) {
  // pair<iterator,bool>
  const auto& result_pair = builder_instances.emplace( splitjoin_name, SplitJoinBuilder{splitjoin_name, pl} );
  if ( ! result_pair.second )
    cerr << "WARNING: splitjoin with name " << splitjoin_name << " already exists, trying to add it more than once" << endl;
  return (result_pair.first)->second;
}
  
SplitJoinBuilder& SplitJoinBuilder::get_builder( const string& splitjoin_name ) {
  auto entry_it = builder_instances.find( splitjoin_name );
  if ( entry_it == builder_instances.end() ) {
    cerr << "ERROR: Could not find splitjoin with name " << splitjoin_name << ", call begin_splitjoin(<name>) prior to any other add_splitjoin_* calls." << endl;
    exit( EXIT_FAILURE );
  }
  assert( entry_it != builder_instances.end() );
  return entry_it->second;
}

void SplitJoinBuilder::remove_builder( const string& splitjoin_name ) {
  auto entry_it = builder_instances.find( splitjoin_name );
  if ( entry_it == builder_instances.end() ) {
    cerr << "ERROR: Could not find splitjoin with name " << splitjoin_name << ", call begin_splitjoin(<name>) prior to any other add_splitjoin_* calls." << endl;
    exit( EXIT_FAILURE );
  }
  assert( entry_it != builder_instances.end() );
  builder_instances.erase( entry_it );  
}


SplitJoin * SplitJoinBuilder::build() {
  if ( ! valid_build_p() ) {
    cerr << "ERROR: attempting to build a splitjoin graph while missing either a count, split or join element" << endl;
    exit( EXIT_FAILURE );
  }
  SplitJoin * splitjoin_obj = new SplitJoin(m_splitjoin_name,
					    m_context,
					    std::move(m_data_collections),
					    std::move(m_stages),
					    m_count,
					    *m_join,
					    *m_split,
					    m_fanin,
					    m_fanout);
  remove_builder( m_splitjoin_name );
  return splitjoin_obj;
}

// ********************************************************************************************

SplitJoin::SplitJoin( const	string& 				splitjoin_name,
		      		pipeline&				pl,
		      		map<string, ChunkedDataCollectionPtr>	data_collections,
		      const 	vector<SplitJoinStage>& 		stages,
		      const 	string&	    				count_command,
		      const 	Stage&			    		join,
		      const 	Stage&			    		split,
		      const	vector<ChunkedDataCollectionRef>	fanin,
		      const	vector<ChunkedDataCollectionRef>	fanout ) :
  m_controlling_tags( pl.m_tags ),
  m_context( pl ),
  m_splitjoin_name( splitjoin_name ),
  m_count_command( count_command ),
  m_tags( pl, splitjoin_name + "_tags" ),
  m_data_collections( data_collections ),
  m_count_collection( pl, m_splitjoin_name + "_count" ),
  // the SplitStage and JoinStage objects need to be constructed in-place here because they need a const reference to a SplitJoin (this)
  m_split(split, *this, fanout),
  m_join(join, *this, fanin),
  m_split_step( pl, splitjoin_name + "_split", m_split, stage_tuner() ),
  m_join_step ( pl, splitjoin_name + "_join",  m_join,  stage_tuner() )
 {
    /** Splitjoin Stages */
    for ( SplitJoinStage stage : stages ) {
      auto stage_step_ptr = std::make_shared<SplitJoinStageColl>( pl, stage.name(), stage );
      if ( VERBOSE_FLAG ) {
	CnC::debug::trace( *stage_step_ptr );
      }
      stage.wire( *stage_step_ptr );
      m_tags.prescribes( *stage_step_ptr, pl );
      m_stage_steps.push_back( stage_step_ptr );
    }

    /** Splitjoin Split */
    m_split.wire( m_split_step );
    m_controlling_tags.prescribes( m_split_step, pl );
    m_split_step.controls( m_tags );
    if ( VERBOSE_FLAG ) {
      CnC::debug::trace( m_split_step );
    }
      
    /** Splitjoin Join */
    m_join.wire( m_join_step );
    m_controlling_tags.prescribes( m_join_step, pl );
    if ( VERBOSE_FLAG ) {
      CnC::debug::trace( m_join_step );
    }
	  
  }


// a parametrizable tuner for items and steps
// we could add depends as long we are working on files
// struct pipe_tuner : public CnC::hashmap_tuner, public CnC::step_tuner<>
// {
//    // returns the steps/stages that consume
//    const vector< stage_id_type > & consumed_by( const int & sfx ) {
//        map_type::const_accessor a;
//        if( consumers.find( a, sfx ) ) {
//            return a->second;
//        } else {
//            static const vector< stage_id_type > _empty;
//            return _empty;
//        }
//    }


// private:
//    typedef tbb::concurrent_hash_map< run_tag_t, vector< stage_id_type > > map_type;
//    tbb::concurrent_hash_map< run_tag_t, vector< stage_id_type > > & consumers;
// };




#endif // _PIPELINE_INCLUDED_
