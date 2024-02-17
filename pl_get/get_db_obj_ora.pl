#!/usr/bin/perl
#
# get_db_obj_ora.pl
#
# SCHEMA OBJECTS UPLOAD (into text-files):
#
# USAGE: get_db_obj_ora.pl <oracle_connect_string> -[<option>...] [ <object_name_REGEXP>|all ] ...
# (oracle_connect_string: username/password@db_name)
#
# 2024-02-17 (last update)

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
  'tab' => ['tab','OT.sql','table'],
  'ix'  => ['tab','OI.sql','table indexes'],
  'trt' => ['tab','OR.sql','triggers'],
  'trv' => ['vie','OR.sql','triggers'],
  'vie' => ['vie','OV.sql','view'],
  'pro' => ['pro','OP.sql','PL/SQL procedure'],
  'fce' => ['fce','OF.sql','PL/SQL function'],
  'pl'  => ['pl', 'OL.sql','PL/SQL package']
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
         if( $Flg_vw ) { $obj_cnt += upload_views( $obj_name_RE, $Flg_vw ) }

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
select rtrim( a.TEXT ) from USER_SOURCE a where a.NAME = ? and a.TYPE = ? order by a.LINE
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
      while( ( $text ) = $c_src->fetchrow_array() ) { print $out $text }

      print $out "\n/\n" ;
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

# --- End of get_db_obj_ora.pl
