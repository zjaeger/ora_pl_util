#!/usr/bin/perl
#
# get_db_obj_ora.pl
#
# SCHEMA OBJECTS UPLOAD (into text-files):
#
# USAGE: get_db_obj_ora.pl <oracle_connect_string> -[<option>...] [ <object_name_REGEXP>|all ] ...
# (oracle_connect_string: username/password@db_name)
#
# 2024-02-27 (last update)

use strict ;
use warnings ;
use integer ;
use File::Basename ;
use DBI ;
use Ora_LDA ;

use open ':encoding(UTF-8)'; # input/output default encoding will be UTF-8
# no warnings 'utf8';

# { <object_type> } => [ <subdir>, <file_postfix>, <file_description> ]
my %h_obj_tp = (
  'tab' => ['tab','O1T.sql','table'],
  'ix'  => ['tab','O1I.sql','table indexes'],
  'trt' => ['tab','O1R.sql','triggers'],
  'trv' => ['vie','O1R.sql','triggers'],
  'vie' => ['vie','O1V.sql','view'],
  'pro' => ['pro','O1P.sql','PL/SQL procedure'],
  'fce' => ['fce','O1F.sql','PL/SQL function'],
  'pl'  => ['pl', 'O1L.sql','PL/SQL package']
) ;

my %h_spath ; # $spath "cache" (optimization only)

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Userid, $Flg_ta, $Flg_vw, $Flg_pl, $ra_Names ) = get_input_params( \@ARGV ) ;
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

         if( $obj_cnt == 0 ) { print STDERR 'Warning: no data found for obj_name_RE = '. $obj_name_RE ."\n" }

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
        @a_name  # view name regexp values
      ) ;
   my @a_val ;

   ( $flg_ta, $flg_vw, $flg_pl ) = ( 0, 0, 0 ) ;
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
            elsif( $ch ne '-')  { print 'Warning: '. $ch .' - invalid option.'."\n" }
           }
        }
      else
        { push( @a_name, $val ) }
     }

   if( scalar @a_name == 0 ) { push( @a_name,'.*') }

   unless(    defined( $userid )
           && scalar @a_name != 0
           && ( $flg_ta != 0 || $flg_vw != 0 || $flg_pl != 0 ) )
     {
      if( scalar @$ra_arg > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .' <userid> [-t] [-v] [-p] [<object_name_RE>] ...'."\n\n" ;
      exit 1 ;
     }

   return ( $userid, $flg_ta, $flg_vw, $flg_pl, \@a_name ) ;
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
   my ( $subdir, $postfix, $descr ) ;
   my ( $spath, $pref ) ;
   my ( $filename, $pathname ) ;

   if( ! exists $h_obj_tp{ $obj_tp } )
     {
      $pathname = $filename = lc( $obj_name ) .'.sql' ;
      $descr = '?' ;
      $postfix = '' ;
      print STDERR 'Warning: '. $obj_tp .' - invalid object type.'."\n" ;
     }
   else
     {
      ( $subdir, $postfix, $descr ) = @{$h_obj_tp{ $obj_tp }} ;

      if( $flg_spath > 0 )
        {
         $spath = '' ;

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
      $filename = lc( $obj_name ) .'_'. $postfix ;
      $pathname = $spath . $filename ;
     }

   return ( $pathname, $filename, $postfix, $descr ) ;
  }


sub out_print_head
  {
   my ( $out, $filename, $file_postfix, $file_desc ) = @_ ;
   my $postfix ;

   $postfix = substr( $file_postfix, 0, index( $file_postfix,'.')) ;

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

   print $out "\n".'TABLESPACE &TABSP_NAME'."\n".'/'."\n\n" ;

   ## print $out " ;\n\n" ;
  }


sub upload_tab_check_con  # -- check constraints
  {
   my ( $out, $table_name ) = @_ ;

   my ( $con, $text ) ;
   my $c_con = $Lda->prepare( q{
select a.CONSTRAINT_NAME, a.SEARCH_CONDITION
from   USER_CONSTRAINTS a
where  a.CONSTRAINT_NAME not like 'SYS%'
       and a.CONSTRAINT_TYPE = 'C'
       and a.TABLE_NAME = ?
order by a.CONSTRAINT_NAME
} ) ;

   $c_con->execute( $table_name ) ;

   while( ( $con, $text ) = $c_con->fetchrow_array() )
     {
      $text =~ s/[\t ]+/ /g ;
      print $out 'ALTER TABLE '. lc( $table_name ) .' ADD'."\n".
                'CONSTRAINT '. lc( $con ) .' CHECK ('."\n".
                ' '. $text .' )'."\n".
                '/'."\n\n" ;
     }
  }


sub upload_tab_indexes
  {
   # $is_iot = 'Y' (index organized table) => vynechava se "primary constraint index"
   my ( $table_name, $flg_spath, $is_iot ) = @_ ;
   my ( $pathname, $filename, $file_postfix, $file_desc ) ;
   my ( $ind_r,  # -- poradove cislo
        $ind_c,  # -- poradove cislo v ramci jednoho indexu
        $sum_c,  # -- pocet sloupcu pro index
        $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name ) ;
   my ( $table_name2, $ind_name2, $ind_type2, $locality2, $ctype2, $col_names ) ;
   my ( $ind_option, $out ) ;

   my $c_ind = $Lda->prepare( q{
select
  row_number() over (order by a.INDEX_NAME, b.COLUMN_POSITION )             as IND_R,
  row_number() over (partition by a.INDEX_NAME order by b.COLUMN_POSITION ) as IND_C,
  count( b.COLUMN_POSITION ) over (partition by a.INDEX_NAME )              as SUM_C,
  a.INDEX_NAME,
  a.INDEX_TYPE,
  a.UNIQUENESS,
  decode( a.PARTITIONED,'yes', d.LOCALITY ) as LOCALITY,
  c.CONSTRAINT_TYPE as CTYPE,
  b.COLUMN_NAME
from
  USER_INDEXES a
  inner join USER_IND_COLUMNS b       on ( a.INDEX_NAME = b.INDEX_NAME )
  left outer join USER_CONSTRAINTS c  on ( a.INDEX_NAME = c.CONSTRAINT_NAME )
  left outer join USER_PART_INDEXES d on ( a.INDEX_NAME = d.INDEX_NAME )
where
  a.TABLE_NAME = ?
  and ('Y' != ? or nvl( c.CONSTRAINT_TYPE,'-') != 'P') -- leaves out PK for index organized table
order by
  a.INDEX_NAME,
  b.COLUMN_POSITION
} ) ;

   $c_ind->execute( $table_name, $is_iot ) ;

   while( ( $ind_r, $ind_c, $sum_c, $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name )
          = $c_ind->fetchrow_array() )
     {
      if( $ind_r == 1 )
        {
         $table_name2 = lc( $table_name ) ;

         ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('ix', $table_name, $flg_spath ) ;

         open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
         out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

          # print $out 'PROMPT Indexes on table '. $table_name ."\n" ;
        }

      if( $ind_c == 1 )  # the first row for the table index
        {
         $ind_name2 = lc( $ind_name ) ;
         $ind_type2 = $ind_type ;
         $locality2 = $locality ;
         $col_names = lc( $col_name ) ;

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
      else
        { $col_names .= ', '. lc( $col_name ) }

      if( $ind_c == $sum_c )  # the last row for the table index
        {
         if( $ctype2 )
           {
            print $out "\n".
                       'ALTER TABLE '. $table_name2 .' ADD'."\n".
                       'CONSTRAINT '. $ind_name2 .' '. $ctype2 .' ( '. $col_names .' )' ;
           }
         else
           {
            if( $ind_type2 ne 'BITMAP')  { $ind_option = '' }
            else
              { $ind_option = $ind_type2 .' ' }

            print $out "\n".
                       'CREATE '. $ind_option .'INDEX '. $ind_name2 .' ON '.
                       $table_name2 .' ( '. $col_names .' )' ;
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

         print $out "\n/\n" ;
        }
     }

   if( $c_ind->rows > 0 )
     {
      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;
     }
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
      left outer join USER_MVIEW_LOGS m on ( a.TABLE_NAME = m.LOG_TABLE )
    where
      regexp_like( a.TABLE_NAME, ?,'i')
      and m.LOG_TABLE is null
      and nvl( a.SECONDARY,'n') != 'Y'
      and a.TABLE_NAME not like 'RUPD$#_%' escape '#'
  ),
X_TABLES_WITH_INDEX
as
  ( select distinct d.TABLE_NAME
    from
      X_TABLES d
      inner join USER_INDEXES e on ( d.TABLE_NAME = e.TABLE_NAME )
  ),
X_TABLES_WITH_TRIGGER
as
  ( select distinct b.TABLE_NAME
    from
      X_TABLES b
      inner join USER_TRIGGERS c on ( b.TABLE_NAME = c.TABLE_NAME )
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
  case when k.TABLE_NAME is not null then 'Y' end as TRIGGER_FLG
from
  X_TABLES i
  left outer join X_TABLES_WITH_INDEX j   on ( i.TABLE_NAME = j.TABLE_NAME )
  left outer join X_TABLES_WITH_TRIGGER k on ( i.TABLE_NAME = k.TABLE_NAME )
order by
  i.TABLE_NAME
} ) ;

   $c_tab->execute( $table_name_IN ) ;

   while( ( $table_name, $tabsp_name, $clust_name, $is_log, $is_part, $is_iot, $is_temp, $duration,
            $index_flg, $trigger_flg ) = $c_tab->fetchrow_array() )
     {
      $out_cnt++ ;
      ( $pathname, $filename, $file_postfix, $file_desc ) = get_pathname('tab', $table_name, $flg_spath ) ;

      open( $out,'>'. $pathname ) || die 'Error on open('. $pathname .'): '. $! ."\n\n" ;
      out_print_head( $out, $filename, $file_postfix, $file_desc ) ;

      # -- table specification
      if( $clust_name )
        { print 'Warning: cluster '. $clust_name .' ignored (table name: '. $table_name .")\n" }

      upload_tab_cols( $out, $table_name, $tabsp_name, $clust_name,
                       $is_log, $is_part, $is_iot, $is_temp, $duration ) ;

      # -- check constraints
      upload_tab_check_con( $out, $table_name ) ;

      # -- comments
      upload_tab_comments( $out, $table_name ) ;

      close( $out ) ; print 'File '. $pathname .' created.'."\n" ;

      # -- indexes (into other file)

      if( $index_flg )   { upload_tab_indexes( $table_name, $flg_spath, $is_iot ) }

      # -- triggers (into other file)

      if( $trigger_flg ) { upload_tab_triggers('trt', $table_name, $flg_spath ) }
     }

   return $out_cnt ;
  }


# --- End of get_db_obj_ora.pl
