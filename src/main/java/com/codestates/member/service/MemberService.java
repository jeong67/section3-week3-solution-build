package com.codestates.member.service;

import com.codestates.exception.BusinessLogicException;
import com.codestates.exception.ExceptionCode;
import com.codestates.member.entity.Member;
import com.codestates.member.repository.MemberRepository;
import com.codestates.stamp.Stamp;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

@Transactional
@Service
public class MemberService {
    private final MemberRepository memberRepository;  // (1)
    private final R2dbcEntityTemplate template;       // (2)
    public MemberService(MemberRepository memberRepository, R2dbcEntityTemplate template) {
        this.memberRepository = memberRepository;
        this.template = template;
    }

    public Mono<Member> createMember(Member member) {
        return verifyExistEmail(member.getEmail())      // (3)
                .then(memberRepository.save(member))    // (4)
                .map(resultMember -> {
                    // Stamp 저장
                    template.insert(new Stamp(resultMember.getMemberId())).subscribe();  // (5)

                    return resultMember;
                });

    }

    public Mono<Member> updateMember(Member member) {
        return findVerifiedMember(member.getMemberId())    // (6)
                .flatMap(updatingMember -> memberRepository.save(updatingMember));    // (8)
    }

    @Transactional(readOnly = true)
    public Mono<Member> findMember(long memberId) {
        return findVerifiedMember(memberId);
    }

    @Transactional(readOnly = true)
    public Mono<Page<Member>> findMembers(PageRequest pageRequest) {
        return memberRepository.findAllBy(pageRequest)  // (9)
                .collectList()     // (10)
                .zipWith(memberRepository.count())   // (11)
                .map(tuple -> new PageImpl<>(tuple.getT1(), pageRequest, tuple.getT2()));  // (12)
    }

    public Mono<Void> deleteMember(long memberId) {
        return findVerifiedMember(memberId)
                .flatMap(member -> template.delete(query(where("MEMBER_ID").is(memberId)), Stamp.class))  // (13)
                .then(memberRepository.deleteById(memberId));              // (14)
    }

    private Mono<Void> verifyExistEmail(String email) {
        return memberRepository.findByEmail(email)
                .flatMap(findMember -> {
                    if (findMember != null) {
                        return Mono.error(new BusinessLogicException(ExceptionCode.MEMBER_EXISTS)); // (15)
                    }
                    return Mono.empty();    // (16)
                });
    }

    private Mono<Member> findVerifiedMember(long memberId) {
        return memberRepository
                .findById(memberId)
                .switchIfEmpty(Mono.error(new BusinessLogicException(ExceptionCode.MEMBER_NOT_FOUND))); // (17)
    }
}