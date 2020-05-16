# vim: ft=python:
# https://github.com/PacificBiosciences/pbbioconda/wiki/IPA-Documentation
import os
print(f'CWD:{os.getcwd()}')

CWD = os.getcwd()
CTG_PREFIX = "ctg."

cfg = config
READS_FN = cfg['reads_fn']
GENOME_SIZE = cfg['genome_size']
COVERAGE = cfg['coverage']
ADVANCED_OPTIONS = cfg["advanced_options"]
POLISH_RUN = 1 if cfg['polish_run'] else 0
PHASING_RUN = 1 if cfg['phase_run'] else 0
NPROC = cfg['nproc']
MAX_NCHUNKS = 40 if 'max_nchunks' not in cfg else cfg['max_nchunks']
TMP_DIR = '/tmp' if 'tmp_dir' not in cfg else cfg['tmp_dir']

LOG_LEVEL = "INFO"
READS_DB_PREFIX = "reads"
CONTIGS_DB_PREFIX = "contigs"

try:
    NPROC_SERIAL = NPROC if not workflow.run_local else NPROC*workflow.cores
except AttributeError:
    # Must be using older snakemake version.
    NPROC_SERIAL = NPROC
print(f'NPROC:{NPROC}')
print(f'NPROC_SERIAL:{NPROC_SERIAL}')


if not os.path.isabs(READS_FN):
    READS_FN = os.path.abspath(os.path.join(CWD, '..', READS_FN))

shell.prefix("set -vxeu -o pipefail; ")
print(f'config:{config}')


localrules: generate_config, ovl_prepare, final

P_CTG_FASTA = 'final/final.p_ctg.fasta'
A_CTG_FASTA = 'final/final.a_ctg.fasta'

rule finish:
    input:
        P_CTG_FASTA,
        A_CTG_FASTA,

rule generate_config:
    output:
        config = "generate_config/generated.config"
    input:
        reads_fn = READS_FN,
    params:
        num_threads = 1, # not needed for localrule, but does not hurt
        genome_size = GENOME_SIZE,
        coverage = COVERAGE,
        advanced_opt_str = ADVANCED_OPTIONS,
        polish_run = POLISH_RUN,
        phase_run = PHASING_RUN,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        mkdir -p generate_config
        cd generate_config
        rel=..

        params_advanced_opt="{params.advanced_opt_str}" \
        params_coverage="{params.coverage}" \
        params_genome_size="{params.genome_size}" \
        params_polish_run="{params.polish_run}" \
        params_phase_run="{params.phase_run}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
        output_fn="generated.config.sh" \
        sentinel_fn="generated.config" \
            time ipa2-task generate_config_from_workflow
    """

rule build_db:
    output:
        seqdb = "build_db/reads.seqdb",
        seeddb = "build_db/reads.seeddb",
        seqdb_seqs = "build_db/reads.seqdb.0.seq",
        seeddb_seeds = "build_db/reads.seeddb.0.seeds",
        input_fofn = "build_db/input.fofn",
    input:
        reads_fn = READS_FN,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        db_prefix = READS_DB_PREFIX,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.seqdb})
        mkdir -p $wd
        cd $wd
        rel=..

        input_reads_fn="{input.reads_fn}" \
        params_db_prefix="{params.db_prefix}" \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_num_threads={params.num_threads} \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task build_db
    """

checkpoint ovl_prepare:
    output:
        blockdir = directory('ovl_prepare/block_ids'),
    input:
        seqdb = rules.build_db.output.seqdb,
    params:
        num_threads = 1, # not needed for localrule, but does not hurt
        max_nchunks = MAX_NCHUNKS,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        rm -f {output.blockdir}
        mkdir -p {output.blockdir}
        cd {output.blockdir}
        rel=../..

        input_db="$rel/{input.seqdb}" \
        output_blocks=./blocks \
        params_max_nchunks="{params.max_nchunks}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task ovl_prepare
    """

def gathered_m4(wildcards):
    checkpoint_output = checkpoints.ovl_prepare.get(**wildcards).output.blockdir  # raises until checkpoint is done
    block_ids = glob_wildcards(os.path.join(checkpoint_output, "{block_id}.txt")).block_id
    return expand("ovl_asym_run/{block_id}/ovl.sorted.m4",
            block_id=block_ids)

rule ovl_asym_run:
    output:
        out_m4 = temp("ovl_asym_run/{block_id}/ovl.m4"),
        out_sorted_m4 = temp("ovl_asym_run/{block_id}/ovl.sorted.m4"),
    input:
        seqdb = rules.build_db.output.seqdb,
        seeddb = rules.build_db.output.seeddb,
        seqdb_seqs = rules.build_db.output.seqdb_seqs,
        seeddb_seeds = rules.build_db.output.seeddb_seeds,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC,
        db_prefix = READS_DB_PREFIX,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        echo "block_id={wildcards.block_id}"
        wd=$(dirname {output.out_m4})
        mkdir -p $wd
        cd $wd
        rel=../..

        # mkdir -p 'ovl_asym_run/{wildcards.block_id}'
        # cd 'ovl_erc_run/{wildcards.block_id}'
        # rel=../..

        input_seqdb="$rel/{input.seqdb}" \
        params_block_id={wildcards.block_id} \
        params_db_prefix="{params.db_prefix}" \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task ovl_asym_run
    """

rule ovl_asym_merge:
    output:
        m4_merged_raw = "ovl_asym_merge/ovl.merged.m4",
        m4_filtered_nonlocal = "ovl_asym_merge/ovl.nonlocal.m4",
    input:
        in_fns = gathered_m4,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        db_prefix = READS_DB_PREFIX,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.m4_merged_raw})
        mkdir -p $wd
        cd $wd
        rel=..

        # We must change rel-path names when we chdir.
        for fn in {input.in_fns}; do
            echo $rel/$fn
        done >| ./merged.fofn

        input_fofn=./merged.fofn \
        params_num_threads="{params.num_threads}" \
        params_db_prefix={params.db_prefix} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task ovl_asym_merge
    """

checkpoint phasing_prepare:
    output:
        blockdir = directory('phasing_prepare/piles'),
    input:
        seqdb = rules.build_db.output.seqdb,
        m4 = rules.ovl_asym_merge.output.m4_filtered_nonlocal,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = 1,
        max_nchunks = MAX_NCHUNKS,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        rm -f {output.blockdir}
        mkdir -p {output.blockdir}
        cd {output.blockdir}
        rel=../..

        output_blocks="blocks" \
        output_blockdir_fn="blockdir_fn.txt" \
        input_m4="$rel/{input.m4}" \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_max_nchunks="{params.max_nchunks}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task phasing_prepare
    """

def gathered_prepared_phasing_m4(wildcards):
    checkpoint_output_ph = checkpoints.phasing_prepare.get(**wildcards).output.blockdir  # raises until checkpoint is done
    block_ids_phasing = glob_wildcards(os.path.join(checkpoint_output_ph, "chunk.{block_id_ph}.m4")).block_id_ph
    return expand("phasing_run/{block_id_ph}/outdir_fn.txt",
            block_id_ph=block_ids_phasing)

rule phasing_run:
    output:
        out_keep_m4 = "phasing_run/{block_id_ph}/ovl.phased.m4",
        out_scraps_m4 = "phasing_run/{block_id_ph}/ovl.phased.m4.scraps",
        out_outdir_fn = "phasing_run/{block_id_ph}/outdir_fn.txt",
    input:
        blockdir = rules.phasing_prepare.output.blockdir,
        config_sh_fn = rules.generate_config.output.config,
        seqdb = rules.build_db.output.seqdb,
        seqdb_seqs = rules.build_db.output.seqdb_seqs,
    params:
        num_threads = NPROC,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        echo "block_id_ph={wildcards.block_id_ph}"
        wd=$(dirname {output.out_keep_m4})
        mkdir -p $wd
        cd $wd
        rel=../..

        input_seqdb="$rel/{input.seqdb}" \
        input_m4="$rel/{input.blockdir}/chunk.{wildcards.block_id_ph}.m4" \
        output_keep_m4=$(basename {output.out_keep_m4}) \
        output_scraps_m4=$(basename {output.out_scraps_m4}) \
        output_outdir_fn="outdir_fn.txt" \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task phasing_run

    """

rule phasing_merge:
    output:
        gathered_m4 = "phasing_merge/ovl.phased.m4",
    input:
        original_m4 = rules.ovl_asym_merge.output.m4_filtered_nonlocal,
        fns = gathered_prepared_phasing_m4,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.gathered_m4})
        mkdir -p $wd
        cd $wd
        rel=..

        # Collect all overlap paths to keep.
        for fn in {input.fns}; do
            # Get the folder name.
            dn=$(cat $rel/$fn)
            echo $dn/ovl.phased.m4
        done >| ./merged.keep.fofn

        # Collect all scraps paths.
        for fn in {input.fns}; do
            # Get the folder name.
            dn=$(cat $rel/$fn)
            echo $dn/ovl.phased.m4.scraps
        done >| ./merged.scraps.fofn

        input_keep_fofn="./merged.keep.fofn" \
        input_scraps_fofn="./merged.scraps.fofn" \
        input_original_m4="$rel/{input.original_m4}" \
        output_m4="ovl.phased.m4" \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task phasing_merge
    """

rule ovl_filter:
    output:
        m4_final = "ovl_filter/ovl.final.m4",
        m4_chimerfilt = "ovl_filter/ovl.chimerfilt.m4",
    input:
        m4 = rules.phasing_merge.output.gathered_m4,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.m4_final})
        mkdir -p $wd
        cd $wd
        rel=..

        input_m4="$rel/{input.m4}" \
        output_m4_final=$(basename {output.m4_final}) \
        output_m4_chimerfilt=$(basename {output.m4_chimerfilt}) \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task ovl_filter
    """

rule assemble:
    output:
        p_ctg_fasta = "assemble/p_ctg.fasta",
        a_ctg_fasta = "assemble/a_ctg.fasta",
        p_ctg_tiling_path = "assemble/p_ctg_tiling_path",
        a_ctg_tiling_path = "assemble/a_ctg_tiling_path",
        p_ctg_fa_fai = "assemble/p_ctg.fasta.fai",
        a_ctg_fa_fai = "assemble/a_ctg.fasta.fai",
        read_to_contig = "assemble/read_to_contig.csv",
#        asm_gfa = "assemble/asm.gfa",
#        sg_gfa = "assemble/sg.gfa",
#        contig_gfa2 = "assemble/contig.gfa2",
        circular_contigs = "assemble/circular_contigs.csv",
    input:
        reads_fn = READS_FN,
        seqdb = rules.build_db.output.seqdb,
        m4 = rules.ovl_filter.output.m4_final,
        m4_phasing_merge = rules.phasing_merge.output.gathered_m4,  # Needed for read tracking.
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        ctg_prefix = CTG_PREFIX,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.p_ctg_fasta})
        mkdir -p $wd
        cd $wd
        rel=..

        input_seqdb="$rel/{input.seqdb}" \
        input_m4="$rel/{input.m4}" \
        input_m4_phasing_merge="$rel/{input.m4_phasing_merge}" \
        input_reads="{input.reads_fn}" \
        params_ctg_prefix={params.ctg_prefix} \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task assemble
    """

#################
### Polishing ###
#################
checkpoint polish_prepare:
    output:
        sharddir = directory('polish_prepare/shards'),
    input:
        read_to_contig = rules.assemble.output.read_to_contig,
        config_sh_fn = rules.generate_config.output.config,
        # The p_ctg and a_ctg FASTA files are needed to enable the
        # config 'polish' option to skip polishing. If polishing is skipped
        # then the output "polished" contigs will be just the draft sequences.
        p_ctg_fasta_fai = rules.assemble.output.p_ctg_fa_fai,
        a_ctg_fasta_fai = rules.assemble.output.a_ctg_fa_fai,
    params:
        num_threads = 1, # not needed for localrule, but does not hurt
        max_nchunks = MAX_NCHUNKS,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        rm -f {output.sharddir}
        mkdir -p {output.sharddir}
        cd {output.sharddir}
        rel=../..

        input_read_to_contig="$rel/{input.read_to_contig}" \
        input_p_ctg_fasta_fai="$rel/{input.p_ctg_fasta_fai}" \
        input_a_ctg_fasta_fai="$rel/{input.a_ctg_fasta_fai}" \
        output_shard_ids=./all_shard_ids \
        output_pwd=./pwd.txt \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_max_nchunks="{params.max_nchunks}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task polish_prepare
    """
def gathered_polish(wildcards):
    checkpoint_output = checkpoints.polish_prepare.get(**wildcards).output.sharddir  # raises until checkpoint is done
    shard_ids_polish = glob_wildcards(os.path.join(checkpoint_output, "shard.{shard_id_polish}.block_ids")).shard_id_polish
    return expand("polish_run/{shard_id_polish}/consensus.fasta",
            shard_id_polish=shard_ids_polish)

rule polish_run:
    output:
        consensus = temp("polish_run/{shard_id_polish}/consensus.fasta"),
    input:
        input_fofn = rules.build_db.output.input_fofn,
        seqdb = rules.build_db.output.seqdb,
        seqdb_seqs = rules.build_db.output.seqdb_seqs,
        p_ctg_fasta = rules.assemble.output.p_ctg_fasta,
        a_ctg_fasta = rules.assemble.output.a_ctg_fasta,
        config_sh = rules.generate_config.output.config, # could be a param if we want to regen
    params:
        num_threads = NPROC,
        polish_prepare_dn = rules.polish_prepare.output.sharddir,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        echo "shard_id={wildcards.shard_id_polish}"
        wd=$(dirname {output.consensus})
        mkdir -p $wd
        cd $wd
        rel=../..

        input_fofn="$rel/{input.input_fofn}" \
        input_seqdb="$rel/{input.seqdb}" \
        input_p_ctg_fasta="$rel/{input.p_ctg_fasta}" \
        input_a_ctg_fasta="$rel/{input.a_ctg_fasta}" \
        output_consensus=$(basename {output.consensus}) \
        params_polish_prepare_dn="$rel/{params.polish_prepare_dn}" \
        params_shard_id={wildcards.shard_id_polish} \
        params_num_threads={params.num_threads} \
        params_config_sh_fn="$rel/{input.config_sh}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task polish_run
    """

rule polish_merge:
    output:
        consensus_merged = "polish_merge/assembly.merged.fasta",
        consensus_merged_fai = "polish_merge/assembly.merged.fasta.fai",
    input:
        fns = gathered_polish,
        p_ctg_fasta = rules.assemble.output.p_ctg_fasta,
        a_ctg_fasta = rules.assemble.output.a_ctg_fasta,
        config_sh_fn = rules.generate_config.output.config,
    params:
        num_threads = NPROC_SERIAL,
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
    shell: """
        wd=$(dirname {output.consensus_merged})
        mkdir -p $wd
        cd $wd
        rel=..

        # We must change rel-path names when we chdir.
        for fn in {input.fns}; do
            echo $rel/$fn
        done >| ./merged.fofn

        input_fofn=./merged.fofn \
        input_p_ctg_fasta="$rel/{input.p_ctg_fasta}" \
        input_a_ctg_fasta="$rel/{input.a_ctg_fasta}" \
        output_fasta=$(basename {output.consensus_merged}) \
        params_num_threads="{params.num_threads}" \
        params_config_sh_fn="$rel/{input.config_sh_fn}" \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
            time ipa2-task polish_merge
    """

rule final:
    output:
        p_ctg_fasta = "final/final.p_ctg.fasta",
        a_ctg_fasta = "final/final.a_ctg.fasta",
    input:
        assembly_merged_fasta = rules.polish_merge.output.consensus_merged,
        assembly_merged_fai = rules.polish_merge.output.consensus_merged_fai,
        p_ctg_fasta = rules.assemble.output.p_ctg_fasta,
        a_ctg_fasta = rules.assemble.output.a_ctg_fasta,
    params:
        num_threads = 1, # not needed for localrule, but does not hurt
        log_level = LOG_LEVEL,
        tmp_dir = TMP_DIR,
        polish_run = POLISH_RUN, 
    shell: """
        wd=$(dirname {output.p_ctg_fasta})
        mkdir -p $wd
        cd $wd
        rel=..

        input_assembly_merged_fasta="$rel/{input.assembly_merged_fasta}" \
        input_p_ctg_fasta="$rel/{input.p_ctg_fasta}" \
        input_a_ctg_fasta="$rel/{input.a_ctg_fasta}" \
        output_p_ctg_fasta=$(basename {output.p_ctg_fasta}) \
        output_a_ctg_fasta=$(basename {output.a_ctg_fasta}) \
        params_log_level="{params.log_level}" \
        params_tmp_dir="{params.tmp_dir}" \
        params_polish_run="{params.polish_run}" \
            time ipa2-task final_collect
    """
