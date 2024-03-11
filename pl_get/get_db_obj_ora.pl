#!/usr/bin/perl
#
# get_db_obj_ora.pl
#
# SCHEMA OBJECTS UPLOAD (into text-files)
#
# USAGE: get_db_obj_ora.pl <oracle_connect_string> -[<option>...] [ <object_name_REGEXP>|all ] ...
# (oracle_connect_string: username/password@db_name)
#
# 2024-03-11 (last update)
#
# TODO: hash partitions (no sys partitions), check constraint - empty row (?), IOT: test

use strict ;
use warnings ;
use integer ;
use File::Basename ;
use DBI ;
use Ora_LDA ;

use open ':encoding(UTF-8)'; # input/output default encoding will be UTF-8
# no warnings 'utf8';

# { <object_type> } => [ <subdir>, <obj_abbrev>, <file_description> ]
#                      [ <subdir>, <obj_abbrev> ] - should be unique
my %h_obj_tp = (
  'tab' => ['tab','T','table'],
  'ix'  => ['tab','I','table indexes'],
  'con' => ['tab','C','table check and FK constraints'],
  'trt' => ['tab','R','triggers'],
  'trv' => ['vie','R','triggers'],
  'vie' => ['vie','V','view'],
  'pro' => ['pro','P','PL/SQL procedure'],
  'fce' => ['fce','F','PL/SQL function'],
  'pl'  => ['pl', 'L','PL/SQL package'],
  'seq' => ['oth','S','sequences']
) ;

my %h_spath ; # $spath "cache" (optimization only)

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Userid, $Flg_ta, $Flg_vw, $Flg_pl, $Flg_sq, $ra_Names ) = get_input_params( \@ARGV ) ;
my $Lda ;

# break flag variable
my $break_FLG = 0 ; $SIG{INT} = sub { $break_FLG = 1 } ;

# connect to Oracle DB
if( $Lda = Ora_LDA::ora_LDA( $Userid ))
  {
   $Lda->{LongReadLen} = 16384*4 ;
   eval
     {
      my $obj_cnt ;

      save_label('00_get_ora_ob.lst') ;

      for my $obj_name_RE ( @$ra_Names )
        {
         $obj_cnt = 0 ;
         if( $Flg_ta ) { $obj_cnt += upload_tables( $obj_name_RE, $Flg_ta ) }
         if( $Flg_vw ) { $obj_cnt += upload_views(  $obj_name_RE, $Flg_vw ) }
         if( $Flg_pl ) { $obj_cnt += upload_plsql(  $obj_name_RE, $Flg_pl ) }
         if( $Flg_sq ) { upload_sequences( $Flg_sq ) }

         if( $obj_cnt == 0 && ( $Flg_ta || $Flg_vw || $Flg_pl ))
           { print STDERR 'Warning: no data found for obj_name_RE = '. $obj_name_RE ."\n" }

         if( $break_FLG != 0 ) { last ; }
        }
     } ;
   if( $@ ) { print STDERR $@ ."\n" }

   if( $break_FLG != 0 ) { print 'BREAK'."\n" }

   # Oracle disconnect
   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_input_params
  {
   my ( $ra_arg ) = @_ ;
   my ( $userid, # Oracle connect string
        $flg_ta, # flag (0, 1, 2, 3): tables
        $flg_vw, # flag (0, 1, 2, 3): views
        $flg_pl, # flag (0, 1, 2, 3): stored code
        $flg_sq, # flag (0, 1, 2)   : sequences
        @a_name  # view name regexp values
      ) ;
   my @a_val ;

   ( $flg_ta, $flg_vw, $flg_pl, $flg_sq ) = ( 0, 0, 0, 0 ) ;
   @a_name = () ;

   foreach my $val ( @$ra_arg )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\S]+$/ )
        # || ( $val =~ /^\w+$/ && exists $ENV{ $val } )
        {
         if( ! defined( $userid )) { $userid = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      elsif( $val =~ /^-/ )
        {
         @a_val = split(//, uc( $val )) ;
         foreach my $ch (@a_val )
           {
            if(    $ch eq 'T' ) { $flg_ta++ }
            elsif( $ch eq 'V' ) { $flg_vw++ }
            elsif( $ch eq 'P' ) { $flg_pl++ }
            elsif( $ch eq 'S' ) { if( $flg_sq < 2 ) { $flg_sq++ }
                                }
            elsif( $ch ne '-')  { print 'Warning: '. $ch .' - invalid option.'."\n" }
           }
        }
      else
        { push( @a_name, $val ) }
     }

   if( scalar @a_name == 0 ) { push( @a_name,'.*') }

   unless(    defined( $userid )
           && scalar @a_name != 0
           && ( $flg_ta != 0 || $flg_vw != 0 || $flg_pl != 0 || $flg_sq != 0 ) )
     {
      if( scalar @$ra_arg > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .' <userid> [-t] [-v] [-p] [-s] [<object_name_RE>] ...'."\n".
                    '  -t+ ... tables (+table indexes, triggers, check/Fk constraints)'."\n".
                    '  -v+ ... views (+view triggers)'."\n".
                    '  -p+ ... PL/SQL stored code (function, procedure, package)'."\n".
                    '  -s+ ... sequences'."\n\n" ;
      exit 1 ;
     }

   return ( $userid, $flg_ta, $flg_vw, $flg_pl, $flg_sq, \@a_name ) ;
  }


sub save_label
  {
   my ( $fname ) = @_ ;
   my ( $sysdate, $uid, $out ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid_text( $Lda ) ;

   open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
   print $out 'Date:   '. $sysdate ."\n".
              'Schema: '. $uid ."\n" ;
   close( $out ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub h_spath_check
  {
   my ( $spath ) = @_ ;

   if( ! exists $h_spath{ $spath } )
     {
      $h_spath{ $spath } = 1 ;
      if( ! -d $spath ) { mkdir $spath }
     }
  }


sub get_pathname
  {
   my ( $obj_tp, $obj_name, $flg_spath ) = @_ ;
   my ( $subdir, $obj_abbrev, $descr ) ;
   my ( $spath, $pref ) ;
   my ( $filename, $pathname, $postfix ) ;

   if( ! exists $h_obj_tp{ $obj_tp } )
     {
      $pathname = $filename = lc( $obj_name ) .'.sql' ;
      $postfix = '' ;
      $descr = '?' ;
      print STDERR 'Warning: '. $obj_tp .' - invalid object type.'."\n" ;
     }
   else
     {
      ( $subdir, $obj_abbrev, $descr ) = @{$h_obj_tp{ $obj_tp }} ;

      if( $flg_spath > 0 )
        {
         $spath = '' ; # sub path

         if( $flg_spath > 1 )
           {
            $spath = $subdir .'/' ;
            h_spath_check( $spath ) ;

            if( $flg_spath > 2 )
              {
               ( $pref ) = $obj_name =~ /^([^\.\_]+)/ ; # up to first '_' or '.'

               $spath .= $pref .'/' ;
               h_spath_check( $spath ) ;
              }
           }
        }
      $postfix = 'O1'. $obj_abbrev ; # O1: O - Oracle, 1 - core
      $filename = lc( $obj_name ) .'_'. $postfix .'.sql' ;
      $pathname = $spath . $filename ;
     }

   return ( $pathname, $filename, $postfix, $descr ) ;
  }


sub out_print_head
  {
   my ( $out, $filename, $postfix, $file_desc ) = @_ ;

   print $out '-- '. $filename ."\n".
              '--'."\n".
              '-- '. (( $postfix ) ? $postfix .': Oracle '. $file_desc
                                   : 'rdbms: oracle') ."\n\n" ;
  }


sub upload_tab_comments  # -- table and column comments
  {
   my ( $out, $table_name ) = @_ ;
   my ( $c_tc1, $c_tc2 ) ;
   my ( $type, $col_name, $text, $first_flg ) ;

   $c_tc1 = $Lda->prepare( q{
select a.TABLE_TYPE, a.COMMENTS
from   USER_TAB_COMMENTS a
where  a.TABLE_NAME = ? and a.COMMENTS is not null
} ) ;
   $c_tc1->execute( $table_name ) ;
   ( $type, $text ) = $c_tc1->fetchrow_array() ;
   $c_tc1->finish() ;

   if( $type )
     {
      $text =~ s/\'/\'\'/g ;
      print $out "\n".'COMMENT ON '. $type .' '. $table_name .' IS'."\n".
                 "\'". $text ."\'"." ;\n" ;
     }

   $c_tc2 = $Lda->prepare( q{
select a.COLUMN_NAME, a.COMMENTS
from   USER_COL_COMMENTS a
where  a.TABLE_NAME = ? and a.COMMENTS is not null
order by a.COLUMN_NAME
} ) ;
   $first_flg = 1 ;
   $c_tc2->execute( $table_name ) ;
   while( ( $col_name, $text ) = $c_tc2->fetchrow_array() )
     {
      if( $first_flg ) { $first_flg = 0 ; print $out "\n" ; }
      $text =~ s/\'/\'\'/g ;
      print $out 'COMMENT ON COLUMN '. $table_name .'.'. $col_name .' IS'."\n".
                 "\'". $text ."\'"." ;\n" ;
     }
  }


sub upload_tab_triggers
  {
   my ( $obj_type, $table_name, $flg_spath ) = @_ ;
   my ( $trg_name, $row_no, $text, $out ) ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;

   my $c_trg = $Lda->prepare( q{
select
  a.TRIGGER_NAME,
  row_number() over (order by a.TRIGGER_NAME ) as ROW_NO
from
  USER_TRIGGERS a
where
  a.TABLE_NAME = ?
  and a.BASE_OBJECT_TYPE = ?
order by
  a.TRIGGER_NAME
} ) ;

   my $c_src = $Lda->prepare( q{
select a.TEXT from USER_SOURCE a where a.NAME = ? and a.TYPE = ? order by a.LINE
} ) ;

   # obj_type = trt - trigger on table
   #            trv - trigger on view
   $c_trg->execute( $table_name, (( $obj_type eq 'trt') ? 'TABLE' : 'VIEW') ) ;

   while( ( $trg_name, $row_no ) = $c_trg->fetchrow_array() )
     {
      if( $row_no == 1 )
        {
         ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname( $obj_type, $table_name, $flg_spath ) ;

         open( $out,'>'. $pathname ) || die 'Error on open ('. $pathname .'): '. $! ."\n\n" ;
         out_print_head( $out, $filename, $file_postfix, $file_desc ) ;
        }

    # print $out 'prompt >>> create trigger '. uc( $trg_name ) ."\n\n" ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $trg_name,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() )
        {
         $text =~ s/\s+$// ; print $out $text ."\n" ;
        }
      print $out "/\n" ;
     }

   if( $c_trg->rows > 0 )
     {
      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;
     }
  }


sub out_text
  {
   my ( $out, $text ) = @_ ;
   my @a_rows = split(/\n/, $text ) ;

   foreach my $row (@a_rows)
     {
      $row =~ s/[\s]+$// ; if( $row ) { print $out "\n". $row }
     }
  }


sub out_text_2
  {
   my ( $out, $text, $view_name ) = @_ ;
   my ( $in, $row ) ;

   ## Sometimes failed:
   ##   Strings with code points over 0xFF may not be mapped into in-memory file handles
   ##   Error: open( text for view <view_name>) failed: Invalid argument
   if( open( $in,'<', \$text ) )
     {
      while( defined( $row = <$in> ) )
        {
         $row =~ s/[\s]+$// ; if( $row ) { print $out "\n". $row }
        }
      close( $in ) ;
     }
   else
     { print STDERR 'Error: open(text for view '. $view_name .') failed: '. $! ."\n" }
  }


sub upload_views
  {
   my ( $view_name_IN, $flg_spath ) = @_ ;
   my $out_cnt = 0 ;
   my ( $view_name, $trigger_flg, $text ) ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my $out ;

   my $c_vie = $Lda->prepare( q{
with
X_VIEWS
as
  ( select a.VIEW_NAME, a.TEXT
    from   USER_VIEWS a
    where  regexp_like( a.VIEW_NAME, ?,'i')
  ),
X_VIEWS_WITH_TRIGGER
as
  ( select distinct b.VIEW_NAME
    from
      X_VIEWS b
      inner join USER_TRIGGERS c on ( b.VIEW_NAME = c.TABLE_NAME )
  )
select
  d.VIEW_NAME,
  case when e.VIEW_NAME is not null then 'Y' end as TRIGGER_FLG,
  d.TEXT
from
  X_VIEWS d
  left outer join X_VIEWS_WITH_TRIGGER e on ( d.VIEW_NAME = e.VIEW_NAME )
order by
  d.VIEW_NAME
} ) ;

   $c_vie->execute( $view_name_IN ) ;

   while( ( $view_name, $trigger_flg, $text ) = $c_vie->fetchrow_array() )
     {
      $out_cnt++ ;
      if( $break_FLG != 0 ) { $c_vie->finish() ; last ; }

      ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('vie', $view_name, $flg_spath ) ;

      open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
      out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

      print $out 'CREATE OR REPLACE VIEW '. lc( $view_name ) ."\n".
                 'AS' ;
      out_text( $out, $text ) ;
      # out_text_2( $out, $text, $view_name ) ;
      print $out "\n;\n" ;

      # -- comments
      upload_tab_comments( $out, $view_name ) ;

      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;

      # -- triggers (into other file)
      if( $trigger_flg ) { upload_tab_triggers('trv', $view_name, $flg_spath ) }
     }

   return $out_cnt ;
  }


sub upload_plsql
  {
   my ( $obj_name_IN, $flg_spath ) = @_ ;
   my $out_cnt = 0 ;
   my ( $ora_obj_tp, $obj_tp, $obj_name ) ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my ( $out, $text, $pb_cnt ) ;

   my $c_pls = $Lda->prepare( q{
select
  a.OBJECT_TYPE,
  case a.OBJECT_TYPE
    when 'FUNCTION'  then 'fce'
    when 'PROCEDURE' then 'pro'
    when 'PACKAGE'   then 'pl'
                     else '?'
  end as OBJ_TYPE,
  a.OBJECT_NAME
from
  USER_OBJECTS a
where
  a.OBJECT_TYPE in ('FUNCTION','PROCEDURE','PACKAGE')
  and regexp_like( a.OBJECT_NAME, ?,'i')
order by
  a.OBJECT_TYPE,
  a.OBJECT_NAME
} ) ;

   my $c_src = $Lda->prepare( q{
select a.TEXT from USER_SOURCE a where a.NAME = ? and a.TYPE = ? order by a.LINE
} ) ;

   $c_pls->execute( $obj_name_IN ) ;
   while( ( $ora_obj_tp, $obj_tp, $obj_name ) = $c_pls->fetchrow_array() )
     {
      $out_cnt++ ;
      if( $break_FLG != 0 ) { $c_pls->finish() ; last ; }

      ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname( $obj_tp, $obj_name, $flg_spath ) ;

      open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
      out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $obj_name, $ora_obj_tp ) ;
      while( ( $text ) = $c_src->fetchrow_array() )
        {
         $text =~ s/\s+$// ; print $out $text ."\n" ;
        }
      print $out "/\n" ;

      if( $obj_tp eq 'pl')
        {
         $pb_cnt = 0 ;
         $c_src->execute( $obj_name,'PACKAGE BODY') ;

         while( ( $text ) = $c_src->fetchrow_array() )
           {
            if( $pb_cnt == 0 ) { print $out "\n".'CREATE OR REPLACE '}
            $pb_cnt++ ;
            $text =~ s/\s+$// ; print $out $text ."\n" ;
           }
         if( $pb_cnt > 0 ) { print $out "/\n" }
        }

      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;
     }

   return $out_cnt ;
  }


sub upload_tab_cols  # table specification
  {
   my ( $out, $table_name, $tabsp_name, $clust_name, $is_log, $is_part, $is_iot,
        $is_temp, $duration ) = @_ ;
# neuplne: nebere v potaz nekt.atributy (tablespace, cluster, ...)
   my ( $col_name, $data_type, $data_len, $data_prec, $data_sc, $data_def, $null ) ;
   my $dtype ;
   my $c_col = $Lda->prepare( q{
select lower(a.COLUMN_NAME), a.DATA_TYPE, a.DATA_LENGTH, a.DATA_PRECISION, a.DATA_SCALE,
       a.DATA_DEFAULT, a.NULLABLE
from   USER_TAB_COLUMNS a
where  a.TABLE_NAME = ?
order by a.COLUMN_ID
} ) ;

 # print $out 'PROMPT Table '. $table_name ."\n\n" ;

   if( $is_temp ne 'Y' )
     { print $out 'CREATE TABLE ' }
   else
     { print $out 'CREATE GLOBAL TEMPORARY TABLE ' }

   print $out lc( $table_name ) ."(" ;

# -- table columns

   $c_col->execute( $table_name ) ;

   while( ( $col_name, $data_type, $data_len, $data_prec, $data_sc,
            $data_def, $null ) = $c_col->fetchrow_array() )
     {
      if( $data_type eq 'NUMBER' )
        {
         if( $data_prec )
           {
            if( $data_sc == 0 )
              { $dtype = $data_type .'('. $data_prec .')' }
                else
              { $dtype = $data_type .'('. $data_prec .','. $data_sc .')' }
           }
         else
           { $dtype = $data_type }
# ----------------------- specialita pro DD: start ---------------------
#          {
#           unless( $col_name =~ /_id$/ )
#             { $dtype = $data_type ; }
#           else
#             { $dtype = $data_type .'(12)' }
#          }
# ----------------------- specialita pro DD: konec ---------------------
        }
      elsif ( $data_type =~ /CHAR/ )
        { $dtype = $data_type .'('. $data_len .')' }
      else
        { $dtype = $data_type }

      if( $c_col->rows > 1 )
        { printf $out ",\n  %-30s  %-20s", $col_name, $dtype }
      else
        { printf $out "\n  %-30s  %-20s", $col_name, $dtype }

      if( defined $data_def )
        {
         $data_def =~ s/[\s]+$// ;
         if( ( $data_def && uc($data_def) ne 'NULL') || $data_def eq '0' )
           {
            print $out ' DEFAULT '. $data_def ;
## -- @@ spec: begin
##       if( $data_def eq '0' )
##         { print 'ALTER TABLE '. lc( $table_name ) .' MODIFY ( '. $col_name .' DEFAULT 0 ) ;'."\n" }
## -- @@ spec: end
           }
        }

      if( $null eq 'Y' )
        { print $out ' NULL' }
      else
        { print $out ' NOT NULL' }
     }

# -- table options

   if( $is_iot ne 'Y' )
     { print $out "\n)" }
   else
     { print_pk_for_iot( $out, $table_name ) }  # -- for index organised table

   if( $is_part eq 'Y' )
     {
      print_partition_clause( $out, $table_name ) ;
     }

   if( $is_temp eq 'Y' )  # -- temporary table
     {
      if( $duration =~ /SESSION$/ )
        { print $out "\n".'ON COMMIT PRESERVE ROWS' }
      else
        { print $out "\n".'ON COMMIT DELETE ROWS' }
     }

   if( $is_log ne 'Y' && $is_temp ne 'Y')  { print $out "\n".'NOLOGGING' }

   print $out "\n".'TABLESPACE &TABSP_NAME'."\n".';'."\n" ;

   ## print $out " ;\n\n" ;
  }


sub upload_tab_con_check
  {
   my ( $out, $table_name ) = @_ ;
   my ( $con, $text, $out_cnt ) ;
   my $c_con = $Lda->prepare( q{
select a.CONSTRAINT_NAME, a.SEARCH_CONDITION
from   USER_CONSTRAINTS a
where  a.CONSTRAINT_NAME not like 'SYS%'
       and a.CONSTRAINT_TYPE = 'C'
       and a.TABLE_NAME = ?
order by a.CONSTRAINT_NAME
} ) ;

   $c_con->execute( $table_name ) ;
   $out_cnt = 0 ;
   while( ( $con, $text ) = $c_con->fetchrow_array() )
     {
      if( $out_cnt == 0 ) { print $out '-- check constraints'."\n" }
      $out_cnt++ ;
      $text =~ s/[\t ]+/ /g ;
      print $out 'ALTER TABLE '. lc( $table_name ) .' ADD'."\n".
                 'CONSTRAINT '.  lc( $con )        .' CHECK ('."\n".
                 ' '. $text .' )'."\n".
                 ';'."\n" ;
     }
   return $out_cnt ;
  }


sub upload_tab_con_FK
  {
   my ( $out, $table_name ) = @_ ;
   my ( $col_seq, $col_cnt, $fk_name, $col_name, $r_tab_name, $r_col_name ) ;
   my ( @a_col_FK, @a_col_PK, $out_cnt ) ;
   my $c_con = $Lda->prepare( q{
select
  row_number() over (partition by a.CONSTRAINT_NAME order by b.POSITION ) as COL_SEQ,
  count( b.POSITION ) over (partition by a.CONSTRAINT_NAME )              as COL_CNT,
--a.TABLE_NAME,
  a.CONSTRAINT_NAME,
--b.POSITION,
  b.COLUMN_NAME,
--a.R_CONSTRAINT_NAME,
  c.TABLE_NAME  as R_TABLE_NAME,
  c.COLUMN_NAME as R_COLUMN_NAME
from
  USER_CONSTRAINTS a
  inner join USER_CONS_COLUMNS b      on (   a.CONSTRAINT_NAME = b.CONSTRAINT_NAME )
  left outer join USER_CONS_COLUMNS c on ( a.R_CONSTRAINT_NAME = c.CONSTRAINT_NAME
                                           and b.POSITION      = c.POSITION )
where
  a.CONSTRAINT_TYPE = 'R'
  and a.TABLE_NAME = ?
order by
  a.CONSTRAINT_NAME,
  b.POSITION
} ) ;

   $out_cnt = 0 ;
   $c_con->execute( $table_name ) ;
   while( ( $col_seq, $col_cnt, $fk_name, $col_name, $r_tab_name, $r_col_name ) = $c_con->fetchrow_array() )
     {
      if( $col_seq == 1 ) { @a_col_FK = @a_col_PK = () }
      push( @a_col_FK, $col_name ) ;
      push( @a_col_PK, $r_col_name ) ;
      if( $col_seq == $col_cnt )
        {
         if( $out_cnt == 0 ) { print $out '-- foreign key constraints'."\n" }
         $out_cnt++ ;
         print $out 'ALTER TABLE '. lc( $table_name ) .' ADD'."\n".
                    'CONSTRAINT '.  lc( $fk_name )    .' FOREIGN KEY ('. join(', ', @a_col_FK ) .")\n".
                    'REFERENCES '.  lc( $r_tab_name )             .' ('. join(', ', @a_col_PK ) .")\n".
                    ';'."\n" ;
        }
     }

   return $out_cnt ;
  }


sub upload_tab_constraints  # -- check and FK constraints
  {
   my ( $table_name, $flg_spath ) = @_ ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my $out ;

   ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('con', $table_name, $flg_spath ) ;

   open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
   out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

   upload_tab_con_check( $out, $table_name ) ;
   upload_tab_con_FK(    $out, $table_name ) ;

   close( $out ) ;
  }


sub upload_tab_indexes
  {
   # $is_iot = 'Y' (index organized table) => "primary constraint index" omitted
   my ( $table_name, $flg_spath, $is_iot ) = @_ ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my ( $col_seq,  # -- index column sequence number
        $col_cnt,  # -- index columns count
        $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name, $col_expr ) ;
   my ( $table_name2, $ind_name2, $ind_type2, $locality2, $ctype2, @a_col_name, $cols_text ) ;
   my ( $ind_option, $first_flg, $fbi_flg, $out ) ;

   my $c_ind = $Lda->prepare( q{
with
X_TAB_INDEXES
as
  ( select
      a.INDEX_NAME,
      a.INDEX_TYPE,
      a.UNIQUENESS,
      decode( a.PARTITIONED,'yes', c.LOCALITY ) as LOCALITY,
      b.CONSTRAINT_TYPE as CTYPE,
      case nvl( b.CONSTRAINT_TYPE,'-')
        when 'P' then 1 -- PK constraint
        when 'U' then 2 -- UK constraint
                 else case when a.UNIQUENESS = 'UNIQUE' then 3 else 4 end
      end as X_PRIO
    from
      USER_INDEXES a
      left outer join USER_CONSTRAINTS b  on ( a.INDEX_NAME = b.CONSTRAINT_NAME )
      left outer join USER_PART_INDEXES c on ( a.INDEX_NAME = c.INDEX_NAME )
    where
      a.TABLE_NAME = ?
      and ('Y' != ? or nvl( b.CONSTRAINT_TYPE,'-') != 'P') -- leaves out PK for table index organized
  )
select
  row_number() over (partition by d.INDEX_NAME order by e.COLUMN_POSITION ) as COL_SEQ,
  count( e.COLUMN_POSITION ) over (partition by d.INDEX_NAME )              as COL_CNT,
  d.INDEX_NAME,
  d.INDEX_TYPE,
  d.UNIQUENESS,
  d.LOCALITY,
  d.CTYPE,
  e.COLUMN_NAME,
  f.COLUMN_EXPRESSION -- LONG (FBI index column expression)
from
  X_TAB_INDEXES d
  inner join USER_IND_COLUMNS e          on ( d.INDEX_NAME = e.INDEX_NAME )
  left outer join USER_IND_EXPRESSIONS f on ( e.INDEX_NAME = f.INDEX_NAME
                                              and e.COLUMN_POSITION = f.COLUMN_POSITION )
order by
  d.X_PRIO,
  d.INDEX_NAME,
  e.COLUMN_POSITION
} ) ;

   $first_flg = 1 ;
   $c_ind->execute( $table_name, $is_iot ) ;

   while( ( $col_seq, $col_cnt, $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name, $col_expr )
          = $c_ind->fetchrow_array() )
     {
      if( $first_flg )
        {
         $first_flg = 0 ;
         $table_name2 = lc( $table_name ) ;

         ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('ix', $table_name, $flg_spath ) ;

         open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
         out_print_head( $out, $filename, $file_postfix, $file_desc ) ;
        }

      if( $col_seq == 1 )  # the first row for the table index
        {
         @a_col_name = () ;
         $fbi_flg = (( $ind_type =~ /^FUNC/ ) ? 1 : 0 ) ;
         $ind_name2 = lc( $ind_name ) ;
         $ind_type2 = $ind_type ;
         $locality2 = $locality ;

         if( $locality2 )  # -- not null only for partitioned index
           {
            if( $locality2 ne 'LOCAL' )
              {
               print 'Warning: partitioned index '. $ind_name .
                     ' with unsuported locality: '. $locality2 ."\n" ;
               $locality2 = '' ;
              }
           }

         unless( $ctype )
           {
# pozn.: pro UNIQUE index, pro kt. neexistuje UNIQUE CONSTRAINT,
# se tento constraint generuje
            if( $uniq ne 'UNIQUE' )  { $ctype2 = '' }
            else
              {
               $ctype2 = $uniq ;
               print 'Warning: UNIQUE index '. $ind_name .' on '. $table_name .
                     ' without according unique constraint exists'."\n" ;
              }
           }
         else
           {
            if( $ctype eq 'P' ) { $ctype2 = 'PRIMARY KEY' } else { $ctype2 = 'UNIQUE' }
           }
        }

      push( @a_col_name, (( $fbi_flg ) ? $col_expr : lc( $col_name )) ) ;

      if( $col_seq == $col_cnt )  # the last row for the table index
        {
         $cols_text = ( $fbi_flg ) ? " (\n". join(",\n", @a_col_name ) ."\n)"
                                   : ' ( '.  join(', ',  @a_col_name ) .' )' ;
         if( $ctype2 )
           {
            print $out 'ALTER TABLE '. $table_name2 .' ADD'."\n".
                       'CONSTRAINT '. $ind_name2 .' '. $ctype2 . $cols_text ;
           }
         else
           {
            if( $ind_type2 ne 'BITMAP')  { $ind_option = '' }
            else
              { $ind_option = $ind_type2 .' ' }

            print $out 'CREATE '. $ind_option .'INDEX '. $ind_name2
                       .' ON '.   $table_name2 . $cols_text ;
           }

         if( $ctype2 )
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print $out "\n".'USING INDEX LOCAL'.
                          "\n".'TABLESPACE &TABSP_NAME' ;
              }
            else
              {
               print $out "\n".'USING INDEX'.
                          "\n".'TABLESPACE &TABSP_NAME' ;
              }
           }
         else
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print $out "\n".'LOCAL'.
                          "\n".'TABLESPACE &TABSP_NAME'.
                          "\n".'PARALLEL' ;
              }
            else
              { print $out "\n".'TABLESPACE &TABSP_NAME' }
           }

         print $out "\n;\n" ;
        }
     }

   if( $c_ind->rows > 0 )
     {
      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;
     }
  }


sub print_pk_for_iot
  {
   my ( $out, $table_name ) = @_ ;
   my ( $con_name, $col_name, $col_pos ) ;

   my $c_pk = $Lda->prepare( q{
select
  lower( a.CONSTRAINT_NAME ),
  lower( b.COLUMN_NAME ),
  b.COLUMN_POSITION
from
  USER_CONSTRAINTS a
  inner join USER_IND_COLUMNS b on ( a.CONSTRAINT_NAME = b.INDEX_NAME )
where
  a.TABLE_NAME = ?
  and a.CONSTRAINT_TYPE = 'P'
order by
  b.COLUMN_POSITION
} ) ;

   $c_pk->execute( $table_name ) ;
   while( ( $con_name, $col_name, $col_pos ) = $c_pk->fetchrow_array() )
     {
      if( $col_pos == 1 )
        {
         print $out ",\n".'CONSTRAINT '. $con_name .' PRIMARY KEY('."\n".
                    '  '. $col_name ;
        }
      else
        { print $out ",\n  ". $col_name }
     }
   print $out " )\n".")\n".'ORGANIZATION INDEX' ;
  }


sub print_partitions
  {
   my ( $out, $table_name, $part_type ) = @_ ;
   my ( $part_name, $val ) ;
   my $keywords ;

   my $c_pa = $Lda->prepare( q{
select a.PARTITION_NAME, a.HIGH_VALUE
from   USER_TAB_PARTITIONS a
where  a.TABLE_NAME = ?
order by a.PARTITION_POSITION
} ) ;

   if(    $part_type eq 'LIST' )  { $keywords = 'VALUES' }
   elsif( $part_type eq 'RANGE')  { $keywords = 'VALUES LESS THAN' }
   elsif( $part_type eq 'HASH' )  { $keywords = '' }
   else                           { $keywords = '/* ERROR (part_type='. $part_type .') */' }

   $c_pa->execute( $table_name ) ;

   while( ( $part_name, $val ) = $c_pa->fetchrow_array() )
     {
      if( $c_pa->rows > 1 ) { print $out ',' }

      if( $keywords )
        { print $out "\n".'  PARTITION '. $part_name .' '. $keywords .' ('. $val .')' }
      else
        { print $out "\n".'  PARTITION '. $part_name }  # -- for hash partitions
     }
  }


sub print_partition_clause
  {
   my ( $out, $table_name ) = @_ ;
   my ( $part_type, $subpart_type, $part_cnt, $col_name ) ;
   my $part_type0 ;

   my $c_pt = $Lda->prepare( q{
select
  a.PARTITIONING_TYPE,
  a.SUBPARTITIONING_TYPE,
  a.PARTITION_COUNT,
  lower( b.COLUMN_NAME )
from
  USER_PART_TABLES a
  inner join USER_PART_KEY_COLUMNS b on ( a.TABLE_NAME = b.NAME
                                          and 'TABLE'  = b.OBJECT_TYPE )
where
  a.TABLE_NAME = ?
order by
  b.COLUMN_POSITION
} ) ;

   $c_pt->execute( $table_name ) ;
   while( ( $part_type, $subpart_type, $part_cnt, $col_name ) = $c_pt->fetchrow_array() )
     {
      if( $c_pt->rows == 1 )
        {
         if( $subpart_type ne 'NONE' )
           {
            print 'Warning: subpartition_type='. $subpart_type .' for table_name='. $table_name ."\n" ;
           }
         print $out "\n".'PARTITION BY '. $part_type .' ('. $col_name ;
         $part_type0 = $part_type ;
        }
      else
        { print $out ', '. $col_name }
     }
   print $out ') (' ;

   print_partitions( $out, $table_name, $part_type0 ) ;

   print $out "\n)" ;
  }


sub upload_tables
  {
   my ( $table_name_IN, $flg_spath ) = @_ ;
   my $out_cnt = 0 ;
   my $out ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my ( $table_name,
        $tabsp_name,  # not used
        $clust_name,  # not used (warning: if not null)
        $is_log,
        $is_part,     # partioned table (Y/N)
        $is_iot,      # index organized table (Y/N)
        $is_temp,     # temporary table (Y/N)
        $duration,    # for temp.table; values: 'SYS$SESSION','SYS$TRANSACTION'
        $index_flg,
        $constr_flg,
        $trigger_flg ) ;
   my $c_tab = $Lda->prepare( q{
with
X_TABLES
as
  ( select
      a.TABLE_NAME,
      a.TABLESPACE_NAME,
      a.CLUSTER_NAME,
      decode( nvl( a.LOGGING,'YES'),'YES','Y','N') as LOGGING,
      decode( a.PARTITIONED,'YES','Y','N')         as PARTITIONED,
      decode( nvl( a.IOT_TYPE,'-'),'IOT','Y','N')  as IOT,
      a.TEMPORARY,
      a.DURATION
    from
      USER_TABLES a
      left outer join USER_MVIEW_LOGS b on ( a.TABLE_NAME = b.LOG_TABLE )
      left outer join USER_MVIEWS m     on ( a.TABLE_NAME = m.MVIEW_NAME )
    where
      regexp_like( a.TABLE_NAME, ?,'i')
      and b.LOG_TABLE  is null         -- no materialized view log
      and m.MVIEW_NAME is null         -- no materialized view
      and nvl( a.SECONDARY,'n') != 'Y' -- f.e.: no spatial index table
      and a.TABLE_NAME not like 'RUPD$#_%' escape '#'
      -- RUPD$: temporary updatable snapshot log created for Java RepAPI
      -- https://forums.oracle.com/ords/apexds/post/materialized-view-log-rupd-8730
  ),
X_TABLES_WITH_INDEX
as
  ( select distinct c.TABLE_NAME
    from
      X_TABLES c
      inner join USER_INDEXES d on ( c.TABLE_NAME = d.TABLE_NAME )
  ),
X_TABLES_WITH_CONSTRAINTS
as
  ( select distinct e.TABLE_NAME
    from
      X_TABLES e
      inner join USER_CONSTRAINTS f on ( e.TABLE_NAME = f.TABLE_NAME )
    where
      f.CONSTRAINT_TYPE in ('C','R')
      and f.CONSTRAINT_NAME not like 'SYS%'
  ),
X_TABLES_WITH_TRIGGER
as
  ( select distinct g.TABLE_NAME
    from
      X_TABLES g
      inner join USER_TRIGGERS h on ( g.TABLE_NAME = h.TABLE_NAME )
  )
select
  i.TABLE_NAME,
  i.TABLESPACE_NAME,
  i.CLUSTER_NAME,
  i.LOGGING,
  i.PARTITIONED,
  i.IOT,
  i.TEMPORARY,
  i.DURATION,
  case when j.TABLE_NAME is not null then 'Y' end as INDEX_FLG,
  case when k.TABLE_NAME is not null then 'Y' end as CONSTR_FLG,
  case when l.TABLE_NAME is not null then 'Y' end as TRIGGER_FLG
from
  X_TABLES i
  left outer join X_TABLES_WITH_INDEX j       on ( i.TABLE_NAME = j.TABLE_NAME )
  left outer join X_TABLES_WITH_CONSTRAINTS k on ( i.TABLE_NAME = k.TABLE_NAME )
  left outer join X_TABLES_WITH_TRIGGER l     on ( i.TABLE_NAME = l.TABLE_NAME )
order by
  i.TABLE_NAME
} ) ;

   $c_tab->execute( $table_name_IN ) ;

   while( ( $table_name, $tabsp_name, $clust_name, $is_log, $is_part, $is_iot, $is_temp, $duration,
            $index_flg, $constr_flg, $trigger_flg ) = $c_tab->fetchrow_array() )
     {
      if( $break_FLG != 0 ) { $c_tab->finish() ; last ; }

      $out_cnt++ ;
      ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('tab', $table_name, $flg_spath ) ;

      open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
      out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

      # -- table specification
      if( $clust_name )
        { print 'Warning: cluster '. $clust_name .' ignored (table name: '. $table_name .")\n" }

      # -- tab columns
      upload_tab_cols( $out, $table_name, $tabsp_name, $clust_name,
                       $is_log, $is_part, $is_iot, $is_temp, $duration ) ;
      # -- comments
      upload_tab_comments( $out, $table_name ) ;

      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;

      # -- indexes (into other file)
      if( $index_flg )   { upload_tab_indexes( $table_name, $flg_spath, $is_iot ) }

      # -- constraints check and FK (into other file)
      if( $constr_flg )  { upload_tab_constraints( $table_name, $flg_spath ) }

      # -- triggers (into other file)
      if( $trigger_flg ) { upload_tab_triggers('trt', $table_name, $flg_spath ) }
     }

   return $out_cnt ;
  }


sub upload_sequences
  {
   my ( $flg_spath ) = @_ ;
   my $out_cnt = 0 ;
   my ( $seq_name, $val_min, $val_max, $incr, $cycle_flg, $order_flg, $cache_size ) ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my $out ;
   my $c_seq = $Lda->prepare( q{
select
  a.SEQUENCE_NAME,
  a.MIN_VALUE,
  a.MAX_VALUE,
  a.INCREMENT_BY,
  a.CYCLE_FLAG,
  a.ORDER_FLAG,
  a.CACHE_SIZE
from
  USER_SEQUENCES a
where
  not regexp_like( a.SEQUENCE_NAME,'^MDRS_\w+\$$')
  and a.SEQUENCE_NAME not like 'SYS%'
order by
  a.SEQUENCE_NAME
} ) ;

   $c_seq->execute() ;
   while( ( $seq_name, $val_min, $val_max, $incr, $cycle_flg, $order_flg, $cache_size )
          = $c_seq->fetchrow_array() )
     {
      if( $break_FLG != 0 ) { $c_seq->finish() ; last ; }

      if( $out_cnt == 0 )
        {
         ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('seq','sequences', $flg_spath ) ;

         open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
         out_print_head( $out, $filename, $file_postfix, $file_desc ) ;
        }
      $out_cnt++ ;

      print $out 'CREATE SEQUENCE '. lc( $seq_name )."\n".
                 (( $incr    == 1 )     ? '' : 'INCREMENT BY '. $incr ."\n") .
                 (( $val_min == 1 )     ? '' : 'MINVALUE '.  $val_min ."\n") .
                 (( $cycle_flg eq 'N' ) ? '' : 'CYCLE'."\n").
                 (( $order_flg eq 'N' ) ? '' : 'ORDER'."\n").
                 (( $cache_size == 0 )  ? 'NOCACHE':'CACHE '. $cache_size )."\n".
                 ";\n" ;
     }

   if( $out_cnt > 0 )
     { close( $out ) ; print 'File '. $pathname .' created.'."\n" ; }
   else
     { print STDERR 'Warning: any sequence not found.'."\n" }
  }


# --- End of get_db_obj_ora.pl
