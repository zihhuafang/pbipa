# IPA HiFi Assembler.
# Authors: Ivan Sovic, Zev Kronenberg, Christopher Dunn, Derek Barnett

version 1.0

workflow ipa2 {
    input {
        File reads_fn
        Int ipa_length_cutoff = -1
        Int ipa_genome_size = 0
        Int ipa_downsampled_coverage = 0
        String ipa_advanced_options = ""
        Int ipa_polish = 0
        Int nproc = 1

        Boolean consolidate_aligned_bam = false
        String? mapping_biosample_name
        String log_level = "INFO"
        Int max_nchunks = 40
        String tmp_dir = "/tmp"
        String ctg_prefix = "ctg."
        String reads_db_prefix = "reads"
        String contigs_db_prefix = "contigs"
    }

    call generate_config {
        input:
            advanced_opt_str = ipa_advanced_options,
            length_cutoff = ipa_length_cutoff,
            downsampled_coverage = ipa_downsampled_coverage,
            genome_size = ipa_genome_size,
            polish = ipa_polish,
    }

    call build_db {
        input:
            reads_fn = reads_fn,
            db_prefix = reads_db_prefix,
            config_sh_fn = generate_config.config_sh_fn,
    }

    call ovl_prepare {
        input:
            seqdb = build_db.seqdb,
    }

    Array[String] blocks = read_lines(ovl_prepare.blocks_fofn)

    scatter (block_id in blocks) {
        call ovl_asym_run {
            input:
                seqdb = build_db.seqdb,
                seeddb = build_db.seeddb,
                seqdb_seqs = build_db.seqdb_seqs,
                seeddb_seeds = build_db.seeddb_seeds,
                config_sh_fn = generate_config.config_sh_fn,
                num_threads = nproc,
                db_prefix = reads_db_prefix,
                block_id = block_id,
        }
    }

    call ovl_asym_merge {
        input:
            in_fns = ovl_asym_run.out_sorted_m4,
            config_sh_fn = generate_config.config_sh_fn,
            num_threads = nproc,
            db_prefix = reads_db_prefix,
    }

    call ovl_filter{
        input:
            m4 = ovl_asym_merge.m4_merged_raw,
            config_sh_fn = generate_config.config_sh_fn,
            num_threads = nproc,
    }

    call assemble {
        input:
            reads_fn = reads_fn,
            seqdb = build_db.seqdb,
            m4 = ovl_filter.m4_final,
            config_sh_fn = generate_config.config_sh_fn,
            ctg_prefix = ctg_prefix,
            num_threads = nproc,
    }

    call build_contig_db {
        input:
            p_ctg_fasta = assemble.p_ctg_fasta,
            a_ctg_fasta = assemble.a_ctg_fasta,
            db_prefix = reads_db_prefix,
            config_sh_fn = generate_config.config_sh_fn,
    }

    call map_all_prepare {
        input:
            seqdb = build_db.seqdb,
            config_sh_fn = generate_config.config_sh_fn,
    }

    Array[String] blocks_map_all = read_lines(map_all_prepare.blocks_fofn)

    scatter (block_id in blocks_map_all) {
        call map_all_run {
            input:
                query_seqdb = build_db.seqdb,
                query_seeddb = build_db.seeddb,
                query_seqdb_seqs = build_db.seqdb_seqs,
                query_seeddb_seeds = build_db.seeddb_seeds,
                target_seqdb = build_contig_db.seqdb,
                target_seeddb = build_contig_db.seeddb,
                target_seqdb_seqs = build_contig_db.seqdb_seqs,
                target_seeddb_seeds = build_contig_db.seeddb_seeds,
                config_sh_fn = generate_config.config_sh_fn,
                num_threads = nproc,
                block_id = block_id,
        }
    }

    call map_all_merge {
        input:
            in_fns = map_all_run.out_m4,
            config_sh_fn = generate_config.config_sh_fn,
            num_threads = nproc,
    }

    call polish_prepare {
        input:
            read_to_contig = map_all_merge.read_to_contig,
            p_ctg_fasta_fai = assemble.p_ctg_fasta_fai,
            a_ctg_fasta_fai = assemble.a_ctg_fasta_fai,
            config_sh_fn = generate_config.config_sh_fn,
    }

    Array[String] blocks_polish = read_lines(polish_prepare.blocks_fofn)

    scatter (block_id in blocks_polish) {
        call polish_run {
            input:
                input_fofn = build_db.input_fofn,
                p_ctg_fasta = assemble.p_ctg_fasta,
                a_ctg_fasta = assemble.a_ctg_fasta,
                blockdir_fn = polish_prepare.blockdir_fn,
                config_sh_fn = generate_config.config_sh_fn,
                num_threads = nproc,
                block_id = block_id,
        }
    }

    call polish_merge {
        input:
            in_fns = polish_run.consensus,
            config_sh_fn = generate_config.config_sh_fn,
            num_threads = nproc,
    }

    output {
        File assembly_merged_fasta = polish_merge.consensus_merged
        File draft_p_ctg_fa = assemble.p_ctg_fasta
        File draft_a_ctg_fa = assemble.a_ctg_fasta
        File circular_contigs = assemble.circular_contigs
    }
}

task generate_config {
    input {
        String advanced_opt_str
        Int length_cutoff
        Int downsampled_coverage
        Int genome_size
        Int polish
    }
    command {
        params_advanced_opt="${advanced_opt_str}" \
        params_subsample_coverage="${downsampled_coverage}" \
        params_genome_size="${genome_size}" \
        params_polish="${polish}" \
        output_fn="generated.config.sh" \
            ipa2-task generate_config_from_workflow
    }
    output {
        File config_sh_fn = "generated.config.sh"
    }
}

task build_db {
    input {
        File reads_fn
        File config_sh_fn
        String db_prefix
    }
    command {
        set -e
        input_reads_fn="${reads_fn}" \
        params_db_prefix="${db_prefix}" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task build_db
    }
    output {
        File seqdb = "${db_prefix}.seqdb"
        File seeddb = "${db_prefix}.seeddb"
        File seqdb_seqs = "${db_prefix}.seqdb.0.seq"
        File seeddb_seeds = "${db_prefix}.seeddb.0.seeds"
        File input_fofn = "input.fofn"
    }
}

task ovl_prepare {
    input {
        String seqdb
    }
    command {
        input_db="${seqdb}" \
        output_blocks=./blocks \
            time ipa2-task ovl_prepare
    }
    output {
        # defer reading this until main workflow resumes (AWS workaround)
        File blocks_fofn = "blocks"
    }
}

task ovl_asym_run {
    input {
        File seqdb
        File seeddb
        File seqdb_seqs
        File seeddb_seeds
        File config_sh_fn
        Int num_threads
        String block_id
        String db_prefix
    }
    command {
        input_seqdb="${seqdb}" \
        params_block_id=${block_id} \
        params_db_prefix=${db_prefix} \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task ovl_asym_run
    }
    runtime {
        cpu: num_threads
    }
    output {
        File out_m4 = "ovl.m4"
        File out_sorted_m4 = "ovl.sorted.m4"
    }
}

task ovl_asym_merge {
    input {
        Array[File] in_fns
        File config_sh_fn
        Int num_threads
        String db_prefix
    }
    command {
        set -vex

        echo ${sep=' ' in_fns} | xargs -n 1 > merged.fofn

        input_fofn=./merged.fofn \
        params_num_threads="${num_threads}" \
        params_db_prefix="${db_prefix}" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task ovl_asym_merge
    }
    output {
        File m4_merged_raw = "ovl.merged.m4"
        File m4_filtered_nonlocal = "ovl.nonlocal.m4"
    }
}

task ovl_filter {
    input {
        File m4
        File config_sh_fn
        Int num_threads
    }
    command {
        set -vex

        input_m4="${m4}" \
        output_m4_final="ovl.final.m4" \
        output_m4_chimerfilt="ovl.chimerfilt.m4" \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task ovl_filter
    }
    output {
        File m4_final = "ovl.final.m4"
        File m4_chimerfilt = "ovl.chimerfilt.m4"
    }
}

task assemble {
    input {
        File reads_fn
        File seqdb
        File m4
        File config_sh_fn
        String ctg_prefix
        Int num_threads
    }

    command {
        input_seqdb="${seqdb}" \
        input_m4="${m4}" \
        input_reads="${reads_fn}" \
        params_ctg_prefix="${ctg_prefix}" \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task assemble
    }
    output {
        File p_ctg_fasta = "p_ctg.fasta"
        File a_ctg_fasta = "a_ctg.fasta"
        File p_ctg_tiling_path = "p_ctg_tiling_path"
        File a_ctg_tiling_path = "a_ctg_tiling_path"
        File p_ctg_fasta_fai = "p_ctg.fasta.fai"
        File a_ctg_fasta_fai = "a_ctg.fasta.fai"
        # File asm_gfa = "asm.gfa"
        # File sg_gfa = "sg.gfa"
        # File contig_gfa2 = "contig.gfa2"
        File circular_contigs = "circular_contigs.csv"
    }
}

######################
### Read tracking. ###
######################
task build_contig_db {
    input {
        File p_ctg_fasta = "p_ctg.fasta"
        File a_ctg_fasta = "a_ctg.fasta"
        File config_sh_fn
        String db_prefix
    }
    command {
        set -e
        input_p_ctg_fasta="${p_ctg_fasta}" \
        input_a_ctg_fasta="${a_ctg_fasta}" \
        params_db_prefix="${db_prefix}" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task build_contig_db
    }
    output {
        File seqdb = "${db_prefix}.seqdb"
        File seeddb = "${db_prefix}.seeddb"
        File seqdb_seqs = "${db_prefix}.seqdb.0.seq"
        File seeddb_seeds = "${db_prefix}.seeddb.0.seeds"
    }
}

task map_all_prepare {
    input {
        String seqdb
        File config_sh_fn
    }
    command {
        input_db="${seqdb}" \
        output_blocks="./blocks" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_prepare
    }
    output {
        # defer reading this until main workflow resumes (AWS workaround)
        File blocks_fofn = "blocks"
    }
}

task map_all_run {
    input {
        File query_seqdb
        File query_seeddb
        File query_seqdb_seqs
        File query_seeddb_seeds
        File target_seqdb
        File target_seeddb
        File target_seqdb_seqs
        File target_seeddb_seeds
        File config_sh_fn
        Int num_threads
        String block_id
    }
    command {
        input_target_seqdb="${target_seqdb}" \
        input_query_seqdb="${query_seqdb}" \
        params_query_block_id=${block_id} \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_run
    }
    runtime {
        cpu: num_threads
    }
    output {
        File out_m4 = "mapped.m4"
    }
}

task map_all_merge {
    input {
        Array[File] in_fns
        File config_sh_fn
        Int num_threads
    }
    command {
        set -vex

        echo ${sep=' ' in_fns} | xargs -n 1 > merged.fofn

        input_fofn=./merged.fofn \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_merge
    }
    output {
        File merged_mapped = "mapped.merged.m4"
        File read_to_contig = "read_to_contig.csv"
    }
}

#################
### Polishing ###
#################
task polish_prepare {
    input {
        File read_to_contig
        File p_ctg_fasta_fai
        File a_ctg_fasta_fai
        File config_sh_fn
    }
    command {
        input_read_to_contig="${read_to_contig}" \
        input_p_ctg_fasta_fai="${p_ctg_fasta_fai}" \
        input_a_ctg_fasta_fai="${a_ctg_fasta_fai}" \
        output_blocks="./blocks" \
        output_blockdir_fn="blockdir_fn.txt" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task polish_prepare
    }
    output {
        File blocks_fofn = "blocks"
        File blockdir_fn = "blockdir_fn.txt"
    }
}

task polish_run {
    input {
        File input_fofn
        File p_ctg_fasta
        File a_ctg_fasta
        File blockdir_fn
        File config_sh_fn
        Int num_threads
        String block_id
    }
    command <<<
        blockdir=$(cat ~{blockdir_fn})
        reads_to_contig_fn="${blockdir}/block_id.~{block_id}.reads"
        contig_id_fn="${blockdir}/block_id.~{block_id}.ctg_id"

        input_fofn="~{input_fofn}" \
        input_p_ctg_fasta="~{p_ctg_fasta}" \
        input_a_ctg_fasta="~{a_ctg_fasta}" \
        output_consensus_fn="consensus.fasta" \
        params_reads_to_contig_fn="${reads_to_contig_fn}" \
        params_ctg_id_fn="${contig_id_fn}" \
        params_block_id=~{block_id} \
        params_num_threads=~{num_threads} \
        params_config_sh_fn="~{config_sh_fn}" \
            time ipa2-task polish_run
    >>>
    runtime {
        cpu: num_threads
    }
    output {
        File consensus = "consensus.fasta"
    }
}

task polish_merge {
    input {
        Array[File] in_fns
        File config_sh_fn
        Int num_threads
    }
    command {
        set -vex

        echo ${sep=' ' in_fns} | xargs -n 1 > merged.fofn

        input_fofn=./merged.fofn \
        output_fasta="assembly.merged.fasta" \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task polish_merge
    }
    output {
        File consensus_merged = "assembly.merged.fasta"
    }
}
