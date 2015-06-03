//********************************************************************************
// Copyright (c) 2013-2015 Yves Vandriessche and Frank Schlimbach.              **
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

// #include <sys/utsname.h>  // for uname()
#include <unistd.h>  // for gethostname()
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <cassert>
#include <libgen.h>
//#include <regex>
//#include <tbb/concurrent_hash_map.h>

// ********************************************************************************************

#define INFINIBAND

#define _DIST_

#ifdef _DIST_
#include <cnc/dist_cnc.h>
#else
#include <cnc/cnc.h>
#endif
#include <cnc/debug.h>

// ********************************************************************************************

const std::string get_hostname() {
  //struct utsname name_cfg;
  //assert( uname( &name_cfg ) == 0 );
  char name_buf[HOST_NAME_MAX];
  assert( gethostname(name_buf, HOST_NAME_MAX) == 0 );
  const std::string hostname(name_buf);
  std::cerr << "Hostname :  " << hostname << std::endl;
#ifdef INFINIBAND
  return hostname + "-ib0";
#else
  return hostname;
#endif
}

//static const std::string _hostname_(get_hostname());

const std::string _local_path_( "/scratch/yvdriess/" );

class File : public CnC::serializable
{
  // we could play some template tricks to save the extra string per data item.
  // depends on a global var, not nice, but might be ok in this context.
  // let's defer that until we see that the additional string per item is actually a problem.
public:
  std::string m_localPath;
  std::string m_name;
  std::string m_postfix;
  File( const std::string &pathname,
	const std::string &name,
	const std::string &postfix ): m_localPath(pathname), m_name(name), m_postfix(postfix) {}
  File( const std::string &name ): m_name(name) {}
  // File( const File& file ): m_localPath( file.m_localPath),
  // 			    m_name(      file.m_name),
  // 			    m_postfix(   file.m_postfix) {}
  // File( File&& file ): m_localPath( std::move(file.m_localPath)),
  // 		       m_name(      std::move(file.m_name)),
  // 		       m_postfix(   std::move(file.m_postfix)) {}
  File( ) = default;

  std::ostream & cnc_format( std::ostream & os ) const {
    os << "file://" << m_localPath << m_name << m_postfix;
  }
  
  //  File& operator=(const File& file) {  }
  void serialize( CnC::serializer & ser )
  {
    ((ser & m_localPath) & m_name) & m_postfix;
    if( m_localPath.empty() ) {
      // not on a local file share -> do nothing
      return;
    }
    std::string _address;
    if( ser.is_packing() )
      _address = get_hostname();
    ser & _address;
    if( ser.is_unpacking() ) {
      std::string filepath( m_localPath + m_name + "." + m_postfix );
      std::cerr << "Moving " << filepath << " from " << _address << " to " << get_hostname() << std::endl;
      const std::string systemcall_str( "rsync -W --ignore-existing " + _address + ":" + filepath + " " + filepath );
      std::cerr << systemcall_str << std::endl << std::flush;
      system( systemcall_str.c_str() );
    }
  }
};

std::ostream & cnc_format( std::ostream & os, const File & f ) {
  return f.cnc_format( os );
}

// ********************************************************************************************

class Stage;
struct stage_tuner;
typedef std::pair< std::string, int > tag_type;
typedef std::string stage_id_type;
typedef CnC::item_collection< tag_type, File > FileCollection;
typedef std::vector< FileCollection* > data_deps_type;
typedef CnC::step_collection< Stage, stage_tuner > StageColl;
typedef std::vector< std::string > deps_names_type;

template< typename INIT > class pipeline;

// ********************************************************************************************
// ********************************************************************************************

struct stage_tuner : public CnC::step_tuner<> {
  template< typename PL >
  int compute_on( const tag_type & tag, PL & pl ) const {
	return tag.second % numProcs();
  }
};

// ********************************************************************************************
// ********************************************************************************************

// A generic stage of a pipeline
// Parametrized by command string and input- and output-collections
// create instance with desired parameters and pass it to constructor of step-collection
class Stage //: public CnC::step_tuner<>
{
public:
  Stage( const std::string & syscall,
	 const data_deps_type & in, const data_deps_type & out,
	 const deps_names_type & in_names, const deps_names_type & out_names );

  // wire the given step-collection with the prod/cons dependences of this step
  void wire( StageColl & sc ) const;

  // note: this must be const, you cannot keep state/mutate "this"
  template< typename PL >
  int execute( const tag_type & file_thunk, PL & pl ) const;

  /* template< class dependency_consumer, class CTX > */
  /* void depends( const tag_type & file_thunk, CTX & arg, dependency_consumer & dC ) const { */
  /*   //(void)arg; */
  /*   //    for( auto i = m_in.begin(); i != m_in.end(); ++i ) */
  /*   //         dC.depends( **i, file_thunk );     */
  /* } */

private:
  const std::string  m_syscall;
  const data_deps_type m_in, m_out;
  const deps_names_type m_in_names, m_out_names;
};

// ********************************************************************************************

// ********************************************************************************************

// A full pipeline
// Created by adding stages with add_stage
// to actually start process, call run for every input data set
template< typename INIT >
class pipeline : public CnC::context< pipeline< INIT > >
{
public:
	pipeline( const std::string cfg = "" );

	// add a parametrized stage with given name
	void add_stage( const Stage & stage, const stage_id_type & name );

	// add a data collection
	FileCollection * add_data_coll( const std::string & name ) ;

	// get data collection with name "name"
	// if it doesn't exist yet, it gets created
	FileCollection * get_data_coll( const std::string & name );

	// start processing a given data set (file)
	void run( const std::string & file_basename, const std::string & dcoll, int file_ID ) ;

#ifdef _DIST_
	void serialize( CnC::serializer & ser );
#endif

private:
	typedef std::map< std::string, FileCollection* > data_coll_map_type;
	typedef std::vector< StageColl * > stage_coll_type;
	data_coll_map_type              m_dataColls;
	stage_coll_type                 m_stages;
	CnC::tag_collection< tag_type > m_tags;
	std::string                     m_cfg;
};

// ********************************************************************************************
// ********************************************************************************************
// ********************************************************************************************

Stage::Stage( const std::string & syscall, const data_deps_type & in, const data_deps_type & out,
	      const deps_names_type & in_names, const deps_names_type & out_names )
  : m_syscall( syscall ),
    m_in( in ),
    m_out( out ),
    m_in_names( in_names ),
    m_out_names( out_names )
{
}

// ********************************************************************************************

// wire the given step-collection with the prod/cons dependences of this step
void Stage::wire( StageColl & sc ) const
{
	for( auto i = m_in.begin(); i != m_in.end(); ++i ) {
		sc.consumes( **i );
	}
	for( auto i = m_out.begin(); i != m_out.end(); ++i ) {
		sc.produces( **i );
	}
}

// ********************************************************************************************

template< typename PL >
int Stage::execute( const tag_type & tag, PL & pl ) const
{
  std::string file_thunk = tag.first;
  int file_id = tag.second;
  // first declare input dependencies by getting input files
  auto in_name_it = m_in_names.begin();
  for( auto i = m_in.begin(); i != m_in.end(); ++i, ++in_name_it ) {
    // 'file' contains the path, basename and postfix of the input files after this point
    // e.g. path = /scratch/
    //      basename = patient0 (=file_thunk)
    //      postfix  = fastq
    File file;
    if( (*i)->unsafe_get( tag, file ) ) {
	  assert( file_thunk == file.m_name );
	  assert( *in_name_it == file.m_postfix );
	}
  }
  pl.flush_gets();
  // now call the program
  //    std::string syscall = std::regex_replace( m_syscall, e, file_thunk );
  std::string testcall = "/bin/echo Executing system command: \"" + m_syscall + "\" | sed 's#{}#" + file_thunk + "#g'";
  std::string syscall = "eval `/bin/echo \"" + m_syscall + "\" | sed 's#{}#" + file_thunk + "#g'`";
  //  std::cerr << syscall << std::endl;
  system( testcall.c_str() );

  int exit_code = 0; // treating non-zero = fail
#ifndef DRYRUN
  exit_code = system( syscall.c_str() );
#endif

  // does not produce any items if the system command failed
  if ( exit_code ) {
    std::cerr << "Command failed, not producing any output items." << std::endl <<
      "\tCommand:\t" << syscall << std::endl <<
      "\tExit code:\t" << exit_code << std::endl;
  }
  else {
    // finally declare/make available output by putting files
    auto out_name_it = m_out_names.begin();
    for ( auto i = m_out.begin();
	  i != m_out.end();
	  ++i, ++out_name_it ) {
      // put the output file, which by convention has the same path and file_thunk name as the input file
      //  the local path part is hard-coded for now
      (*i)->put( tag, File(_local_path_, file_thunk, *out_name_it) );
    }
  }
  return CnC::CNC_Success;
}

// ********************************************************************************************
// ********************************************************************************************



// ********************************************************************************************
// ********************************************************************************************

template< typename INIT >
pipeline< INIT>::pipeline( const std::string cfg )
	: m_dataColls(),
	m_stages(),
	m_tags( *this ),
	m_cfg( cfg )
{
  //  CnC::debug::trace_all( *this );
    
  if( m_cfg.size() ) INIT( *this, m_cfg );
}

// ********************************************************************************************

template< typename INIT >
void pipeline< INIT>::add_stage( const Stage & stage, const stage_id_type & name )
{
  StageColl * sc = new StageColl( *this, name, stage, stage_tuner() );
	stage.wire( *sc );
	m_stages.push_back( sc );
	m_tags.prescribes( *sc, *this );
	CnC::debug::trace( *sc );
	CnC::Internal::Speaker os; os << "Added stage " << name;
}

// ********************************************************************************************

// add a data collection
template< typename INIT >
FileCollection * pipeline< INIT>::add_data_coll( const std::string & name )
{
  FileCollection * fc = new FileCollection( *this, name );
  m_dataColls.insert( data_coll_map_type::value_type( name, fc ) );
  //CnC::debug::trace( *fc );
  CnC::Internal::Speaker os; os << "Added data_coll " << name;
  return fc;
}

// ********************************************************************************************

// get data collection with name "name"
template< typename INIT >
FileCollection * pipeline< INIT>::get_data_coll( const std::string & name )
{
	auto _dc = m_dataColls.find( name );
	if( _dc == m_dataColls.end() ) {
	  //		std::cerr << "Couldn't find data-coll " << name << ", creating default collection" << std::endl;
		return add_data_coll( name );
	}
	return _dc->second;
}

// ********************************************************************************************

// start processing a given data set (file)
template< typename INIT >
void pipeline< INIT>::run( const std::string & basename, const std::string & dcoll, int file_id )
{
  const tag_type tag( basename, file_id );
  const std::string pathname( _local_path_ );
  //  const std::string basename( tag.first );
  const std::string  postfix( dcoll );
  // we rely on a file fetching stage to push the first file in based solely on the tag (file basename)
  //  get_data_coll( dcoll )->put( tag, File(pathname, basename, postfix) );
  m_tags.put( tag );
}

// ********************************************************************************************

#ifdef _DIST_
template< typename INIT >
void pipeline< INIT >::serialize( CnC::serializer & ser )
{
	ser & m_cfg;
	// remotely we need to init the pipeline from the cfg string
	if( ser.is_unpacking() ) INIT( *this, m_cfg );
}
#endif


// ********************************************************************************************



// a parametrizable tuner for items and steps
// we could add depends as long we are working on files
// struct pipe_tuner : public CnC::hashmap_tuner, public CnC::step_tuner<>
// {
//    // returns the steps/stages that consume
//    const std::vector< stage_id_type > & consumed_by( const int & sfx ) {
//        map_type::const_accessor a;
//        if( consumers.find( a, sfx ) ) {
//            return a->second;
//        } else {
//            static const std::vector< stage_id_type > _empty;
//            return _empty;
//        }
//    }


// private:
//    typedef tbb::concurrent_hash_map< tag_type, std::vector< stage_id_type > > map_type;
//    tbb::concurrent_hash_map< tag_type, std::vector< stage_id_type > > & consumers;
// };




#endif // _PIPELINE_INCLUDED_
